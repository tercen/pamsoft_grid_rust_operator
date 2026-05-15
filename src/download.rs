//! Stage 4: file download + ZIP extraction.
//!
//! For each chip group (keyed by primary documentId) in
//! [`crate::input::InputData`], we fetch every unique `documentId` it
//! references via tercen-rs' `FileService.download` streaming RPC,
//! unzip into a per-group dir under the temp root, and locate the
//! TIFFs + array-layout file. The output is a
//! `BTreeMap<String, GroupFiles>` keyed by primary documentId — exactly
//! the shape stage 5 (algorithm invocation) wants.
//!
//! Mirrors `aux_functions.R::prep_image_folder`'s contract:
//! - The first documentId is the **image ZIP**.
//! - The second documentId, if present, is the **array-layout text file**.
//!   (When absent, the layout file is expected to live *inside* the ZIP.)
//!
//! Downloads run sequentially per chip and are deduplicated within the
//! whole input: if two groups share a documentId we only fetch the bytes
//! once. The temp dir is the caller's responsibility — it should clean
//! up after the algorithm finishes.
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

/// Where each group's input data ended up on disk after download +
/// extraction. Stage 5 builds one `pamsoft_grid::types::GroupConfig`
/// per entry.
#[derive(Debug, Clone)]
pub struct GroupFiles {
    /// Primary documentId for this chip group — the image ZIP's id.
    /// Identifies the group and matches the key in the `BTreeMap`
    /// returned by [`download_all_groups`].
    pub doc_id: String,
    /// `.ci` values of the rows that make up this group (one per image).
    /// Same order as `image_paths`; stage 6 uses these to tag output spots.
    pub cis: Vec<i32>,
    /// Absolute paths to the TIFFs for this group, in column-facet row
    /// order (matches `cis`).
    pub image_paths: Vec<PathBuf>,
    /// Absolute path to the `* Array Layout*.txt` file — either supplied
    /// as a separate documentId or located inside the image ZIP.
    pub layout_path: PathBuf,
}

/// Download every documentId referenced by the input, extract each ZIP
/// once, and resolve the per-group image + layout paths.
///
/// `work_root` is a caller-owned temp dir (e.g. via [`tempfile::tempdir`])
/// that will hold a `doc/<documentId>/` subtree per downloaded document.
pub async fn download_all_groups(
    ctx: &ContextBase,
    input: &InputData,
    work_root: &Path,
) -> Result<BTreeMap<String, GroupFiles>> {
    std::fs::create_dir_all(work_root)
        .with_context(|| format!("create work root {}", work_root.display()))?;

    // Deduplicate: each documentId is downloaded at most once across all
    // groups. doc_extract_dirs[id] → path of the extracted-or-raw doc.
    let mut doc_paths: HashMap<String, DownloadedDoc> = HashMap::new();
    for row in input.groups.values().flatten() {
        for doc_id in &row.document_ids {
            if doc_paths.contains_key(doc_id) {
                continue;
            }
            let dir = work_root.join("doc").join(doc_id);
            std::fs::create_dir_all(&dir).with_context(|| {
                format!("create doc dir {}", dir.display())
            })?;
            let doc = fetch_and_unpack(ctx, doc_id, &dir).await?;
            doc_paths.insert(doc_id.clone(), doc);
        }
    }

    // Per-group resolution: pick the image files and the layout out of
    // the downloaded docs.
    let mut groups = BTreeMap::new();
    for (doc_id, rows) in &input.groups {
        // Sanity: every row in a group should reference the same set of
        // documentIds (they all share the primary doc_id, but verify
        // the secondary layout-doc-id agrees too).
        let first_docs = &rows[0].document_ids;
        for r in rows.iter().skip(1) {
            if r.document_ids != *first_docs {
                bail!(
                    "rows within chip group doc_id={} disagree on full \
                     documentId tuple: {:?} vs {:?}. The primary doc_id matches \
                     by construction; the secondary (layout) doc_id must too.",
                    doc_id, first_docs, r.document_ids
                );
            }
        }

        let image_doc = doc_paths
            .get(&first_docs[0])
            .ok_or_else(|| anyhow!("missing download for doc {}", first_docs[0]))?;
        let image_root = image_doc.extracted_root.as_deref().ok_or_else(|| {
            anyhow!(
                "documentId {} did not unzip — pamsoft expects an image ZIP \
                 as the first documentId column",
                first_docs[0]
            )
        })?;

        // Pull TIFFs out of the extracted image-ZIP tree by matching each
        // row's label_factor (filename stem) against `image_root/**/*.tif`.
        let tiff_index = index_tiffs(image_root)?;
        let mut image_paths = Vec::with_capacity(rows.len());
        let mut cis = Vec::with_capacity(rows.len());
        for row in rows {
            let path = tiff_index.get(&row.image_label).ok_or_else(|| {
                anyhow!(
                    "chip doc_id={}, row .ci={}, label '{}': no TIFF matching that filename stem under {}. \
                     Available stems: {}",
                    doc_id,
                    row.ci,
                    row.image_label,
                    image_root.display(),
                    tiff_index.keys().take(5).cloned().collect::<Vec<_>>().join(", "),
                )
            })?;
            image_paths.push(path.clone());
            cis.push(row.ci);
        }

        // Layout: second documentId if present (separate text file),
        // otherwise look inside the image ZIP.
        let layout_path = if first_docs.len() == 2 {
            let layout_doc = doc_paths
                .get(&first_docs[1])
                .ok_or_else(|| anyhow!("missing download for layout doc {}", first_docs[1]))?;
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
                        "second documentId {} present but neither raw file nor \
                         a *Array Layout*.txt found inside",
                        first_docs[1]
                    )
                })?
        } else {
            locate_layout_file(image_root).ok_or_else(|| {
                anyhow!(
                    "no separate layout documentId and no *Array Layout*.txt \
                     found inside the image ZIP at {}",
                    image_root.display()
                )
            })?
        };

        groups.insert(
            doc_id.clone(),
            GroupFiles {
                doc_id: doc_id.clone(),
                cis,
                image_paths,
                layout_path,
            },
        );
    }

    Ok(groups)
}

/// A single downloaded documentId — either an extracted ZIP (then
/// `extracted_root` is set) or a non-archive file (then `raw_file` is set).
#[derive(Debug, Clone)]
struct DownloadedDoc {
    extracted_root: Option<PathBuf>,
    raw_file: Option<PathBuf>,
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
        Ok(DownloadedDoc {
            extracted_root: Some(extracted),
            raw_file: None,
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
/// insensitive) and ends in `.txt`. Mirrors `bulk_regression`'s
/// `locate_images_and_layout` heuristic at `src/bin/bulk_regression.rs:386-398`.
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
