//! Stage 4: file download + ZIP extraction.
//!
//! Each `.ci` is one chip (matching R), but multiple `.ci`s typically
//! share the same image ZIP (one ZIP holds many TIFFs across cycles ×
//! exposures). We download each unique documentId once and index its
//! TIFFs by filename stem; stage 5 then resolves per-row paths from the
//! cache.
//!
//! Mirrors `aux_functions.R::prep_image_folder`'s contract:
//! - The first documentId is the **image ZIP**.
//! - The second documentId, if present, is the **array-layout text file**.
//!   (When absent, the layout file is expected to live *inside* the ZIP.)
//!
//! No fallbacks: gRPC errors, malformed ZIPs, missing TIFFs all bubble
//! up as `anyhow::Error` so the Tercen task surfaces a clear failure
//! reason rather than silently dropping data.

use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use tercen_rs::context::ContextBase;
use tonic::Request;

use crate::input::InputData;

/// Where each downloaded documentId ended up on disk after fetch +
/// (optional) ZIP extraction.
#[derive(Debug, Clone)]
pub struct DownloadedDoc {
    /// Set when the bytes were a ZIP archive — points at the extracted
    /// root directory.
    pub extracted_root: Option<PathBuf>,
    /// Set when the bytes were *not* a ZIP — points at the raw file.
    pub raw_file: Option<PathBuf>,
    /// `{filename_stem → absolute path}` for every `*.tif` found
    /// recursively inside `extracted_root`. Empty if `extracted_root`
    /// is `None`. Stage 5 looks up each row's `image_label` here to get
    /// the TIFF path.
    pub tiff_index: HashMap<String, PathBuf>,
}

/// Catalogue of every downloaded documentId. Keyed by documentId so the
/// algorithm stage can look up the image ZIP (for TIFFs) and the
/// optional layout-file doc (for the array-layout text) by id.
pub type DocumentCatalogue = BTreeMap<String, DownloadedDoc>;

/// Download every unique documentId in `input`, extract the ZIPs, and
/// index their TIFFs. Output is the per-doc catalogue plus the resolved
/// path of the **layout file** — either a separate documentId or the
/// `*Array Layout*.txt` discovered inside the first image-ZIP we see.
pub async fn download_all(
    ctx: &ContextBase,
    input: &InputData,
    work_root: &Path,
) -> Result<(DocumentCatalogue, PathBuf)> {
    std::fs::create_dir_all(work_root)
        .with_context(|| format!("create work root {}", work_root.display()))?;

    // 1) Fetch + unpack every unique documentId in the input. Within one
    // input the same ZIP doc-id is referenced by many .ci rows; download
    // it once.
    let mut catalogue: DocumentCatalogue = BTreeMap::new();
    for doc_id in input.unique_document_ids() {
        let dir = work_root.join("doc").join(&doc_id);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create doc dir {}", dir.display()))?;
        let doc = fetch_and_unpack(ctx, &doc_id, &dir).await?;
        catalogue.insert(doc_id, doc);
    }

    // 2) Resolve the layout file. All rows in the input should agree on
    // which doc-id supplies the layout (it's the second documentId if
    // present, else it lives inside the image ZIP).
    let layout_path = resolve_layout(input, &catalogue)?;

    Ok((catalogue, layout_path))
}

/// Resolve the array-layout `.txt` path. Two strategies, matching the R
/// operator (`aux_functions.R::prep_image_folder`):
///
/// 1. **Two documentId columns** → the second documentId is the layout
///    file. Its `DownloadedDoc.raw_file` is the layout, or if the bytes
///    happened to be a ZIP, we look for `*Array Layout*.txt` inside.
/// 2. **One documentId column** → the layout lives inside the image
///    ZIP. We probe the first row's image-ZIP doc and walk its
///    extracted tree for the first match.
fn resolve_layout(input: &InputData, catalogue: &DocumentCatalogue) -> Result<PathBuf> {
    let first = input
        .rows
        .first()
        .ok_or_else(|| anyhow!("no input rows — cannot resolve layout"))?;

    if first.document_ids.len() == 2 {
        let layout_doc_id = &first.document_ids[1];
        // Sanity: every row must agree on the secondary doc-id (the
        // primary can vary — that's just the image ZIP — but the layout
        // doc-id is per-input).
        for r in &input.rows {
            if r.document_ids.get(1) != Some(layout_doc_id) {
                bail!(
                    "row .ci={} disagrees on layout documentId ({:?} vs {:?}). \
                     All rows must share the same secondary documentId.",
                    r.ci,
                    r.document_ids.get(1),
                    Some(layout_doc_id),
                );
            }
        }
        let layout_doc = catalogue.get(layout_doc_id).ok_or_else(|| {
            anyhow!("layout documentId {} was not downloaded", layout_doc_id)
        })?;
        layout_doc
            .raw_file
            .clone()
            .or_else(|| {
                layout_doc
                    .extracted_root
                    .as_deref()
                    .and_then(locate_layout_file)
            })
            .ok_or_else(|| {
                anyhow!(
                    "secondary documentId {} present but neither a raw layout file \
                     nor a *Array Layout*.txt was found inside",
                    layout_doc_id,
                )
            })
    } else {
        let image_doc_id = &first.document_ids[0];
        let image_doc = catalogue
            .get(image_doc_id)
            .ok_or_else(|| anyhow!("image documentId {} was not downloaded", image_doc_id))?;
        let root = image_doc.extracted_root.as_deref().ok_or_else(|| {
            anyhow!(
                "documentId {} did not unzip — pamsoft expects an image ZIP \
                 as the (sole) documentId column when no separate layout is provided",
                image_doc_id,
            )
        })?;
        locate_layout_file(root).ok_or_else(|| {
            anyhow!(
                "no separate layout documentId and no *Array Layout*.txt \
                 found inside the image ZIP at {}",
                root.display(),
            )
        })
    }
}

/// Pull `doc_id` over gRPC, write it to `dir`, and either unzip it if it
/// looks like a ZIP archive or leave it as-is.
async fn fetch_and_unpack(
    ctx: &ContextBase,
    doc_id: &str,
    dir: &Path,
) -> Result<DownloadedDoc> {
    tracing::info!(doc_id, "downloading file");
    let bytes = stream_file_bytes(ctx, doc_id).await?;
    tracing::info!(doc_id, bytes = bytes.len(), "download complete");

    // ZIP magic number is `PK\x03\x04` (or `PK\x05\x06` for empty).
    let looks_like_zip =
        bytes.len() >= 4 && (&bytes[..4] == b"PK\x03\x04" || &bytes[..4] == b"PK\x05\x06");

    if looks_like_zip {
        let extracted = dir.join("extracted");
        std::fs::create_dir_all(&extracted)?;
        extract_zip(&bytes, &extracted).with_context(|| {
            format!("extract zip for doc {} into {}", doc_id, extracted.display())
        })?;
        let tiff_index = index_tiffs(&extracted)?;
        Ok(DownloadedDoc {
            extracted_root: Some(extracted),
            raw_file: None,
            tiff_index,
        })
    } else {
        // Non-archive — write the raw bytes to disk so the algorithm can
        // open it. The second-documentId case (separate layout file) hits
        // this path.
        let path = dir.join("file");
        let mut f = std::fs::File::create(&path)?;
        f.write_all(&bytes)?;
        Ok(DownloadedDoc {
            extracted_root: None,
            raw_file: Some(path),
            tiff_index: HashMap::new(),
        })
    }
}

/// Stream `FileService::download(file_document_id)` to completion and
/// concatenate the chunks into one Vec<u8>. For our scale (~100 MB ZIPs)
/// the all-in-memory approach is fine; if we ever stream-extract directly
/// we'd swap the buffer here for a writer.
async fn stream_file_bytes(ctx: &ContextBase, doc_id: &str) -> Result<Vec<u8>> {
    use tercen_rs::client::proto::ReqDownload;

    let mut file_service = ctx
        .client()
        .file_service()
        .map_err(|e| anyhow!("acquire file service: {e}"))?;

    let req = Request::new(ReqDownload {
        file_document_id: doc_id.to_string(),
    });
    let mut stream = file_service
        .download(req)
        .await
        .map_err(|e| anyhow!("file_service.download({doc_id}) failed: {e}"))?
        .into_inner();

    let mut buf = Vec::new();
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| anyhow!("stream chunk for {doc_id}: {e}"))?
    {
        buf.extend_from_slice(&chunk.result);
    }
    if buf.is_empty() {
        bail!("documentId {} download returned 0 bytes", doc_id);
    }
    Ok(buf)
}

/// Extract a ZIP archive (in-memory) into `dest`. No security tricks —
/// we trust the upstream source (Tercen-stored production data).
fn extract_zip(bytes: &[u8], dest: &Path) -> Result<()> {
    let reader = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader).context("open zip archive")?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .with_context(|| format!("zip entry {i}"))?;
        let name = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("zip entry {i} has invalid name"))?
            .to_path_buf();
        let outpath = dest.join(&name);
        if entry.is_dir() {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(parent) = outpath.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut out = std::fs::File::create(&outpath)?;
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            out.write_all(&data)?;
        }
    }
    Ok(())
}

/// Walk an extracted-ZIP directory, return `{filename_stem → full path}`
/// for every `*.tif` it contains. Stems are matched against the input
/// rows' `image_label` field.
fn index_tiffs(root: &Path) -> Result<HashMap<String, PathBuf>> {
    let mut out = HashMap::new();
    walk(root, &mut |path| {
        let is_tif = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("tif") || e.eq_ignore_ascii_case("tiff"))
            .unwrap_or(false);
        if !is_tif {
            return;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            out.insert(stem.to_string(), path.to_path_buf());
        }
    })?;
    Ok(out)
}

/// First file under `root` whose name contains "Array Layout" (case-
/// insensitive) and ends in `.txt`.
fn locate_layout_file(root: &Path) -> Option<PathBuf> {
    let mut found = None;
    let _ = walk(root, &mut |path| {
        if found.is_some() {
            return;
        }
        let ends_txt = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("txt"))
            .unwrap_or(false);
        if !ends_txt {
            return;
        }
        let name_lower = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.to_lowercase())
            .unwrap_or_default();
        if name_lower.contains("array layout") {
            found = Some(path.to_path_buf());
        }
    });
    found
}

/// Tiny recursive walker (no walkdir dep). Calls `f` once per non-dir
/// entry. Returns the first IO error encountered.
fn walk<F>(root: &Path, f: &mut F) -> Result<()>
where
    F: FnMut(&Path),
{
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("read dir {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                f(&path);
            }
        }
    }
    Ok(())
}
