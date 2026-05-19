//! Stage 5: algorithm invocation.
//!
//! For each downloaded chip group, build a `pamsoft_grid::types::GroupConfig`
//! and call `pamsoft_grid::batch::process_single_group`, returning the
//! per-spot `SpotResult` rows. This is the bit that replaces "shell out
//! to MATLAB MCR" in the original R operator.
//!
//! No upload yet — stage 6 (`output.rs`) and stage 7 (`upload.rs`) cover
//! the result-table construction and `save_table` round-trip.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use pamsoft_grid::batch::process_single_group;
use pamsoft_grid::io::load_tiff_image;
use pamsoft_grid::types::{GroupConfig, ImageType, SpotResult};

use crate::download::GroupFiles;
use crate::props::PamsoftProps;

/// One group's grid-detection output, tagged with its primary
/// documentId and the per-image `.ci`s so stage 6 can stitch the
/// result table rows back together (one row per spot × per image).
pub struct GroupResult {
    /// Primary documentId (the image ZIP) — the chip group's key.
    pub doc_id: String,
    /// Per-image `.ci` values, parallel to the images that produced
    /// the spots. One entry per image in this group.
    pub cis: Vec<i32>,
    /// Per-image filename stems, parallel to `cis`. Stage 6 maps a
    /// `SpotResult.image_name` (also a filename stem) back to its `.ci`
    /// via this Vec — mirroring the R operator's `filter(get(imageCol)
    /// == griddingOutput$grdImageNameUsed[1]) %>% pull(.ci)`.
    pub image_labels: Vec<String>,
    pub spots: Vec<SpotResult>,
    /// Effective spot pitch used (after auto-detection if the user left
    /// `Spot Pitch = 0`). Surfaced for logging / debugging.
    pub spot_pitch: f64,
}

/// Run the grid algorithm on every chip group. Returns one
/// `GroupResult` per documentId in input order.
pub fn run_grid_per_group(
    groups: &std::collections::BTreeMap<String, GroupFiles>,
    props: &PamsoftProps,
) -> Result<Vec<GroupResult>> {
    let mut out = Vec::with_capacity(groups.len());
    for (doc_id, files) in groups {
        let spot_pitch = if props.spot_pitch > 0.0 {
            props.spot_pitch
        } else {
            // Auto-detect from the first image's dimensions, matching
            // the R operator's `get_imageset_type` fallback
            // (aux_functions.R:148-156). 552×413 → 17.0 (Evolve3),
            // 697×520 → 21.5 (Evolve2). Anything else errors loud.
            autodetect_spot_pitch(&files.image_paths[0])
                .with_context(|| format!("doc_id={doc_id}: auto-detect spot pitch"))?
        };

        let group = GroupConfig {
            group_id: doc_id.clone(),
            min_diameter: props.min_diameter,
            max_diameter: props.max_diameter,
            edge_sensitivity: props.edge_sensitivity.to_vec(),
            series_mode: 0,
            show_viewer: 0,
            spot_pitch,
            spot_size: props.spot_size,
            rotation: props.rotation.clone(),
            saturation_limit: props.saturation_limit,
            seg_method: props.seg_method.clone(),
            // R operator uses "Last" for the gridding reference image;
            // our parity work confirmed the algorithm is stable across
            // First/Last choice once preprocessing is wired in
            // (GRID_PERFORMANCE.md). Sticking with "Last" for parity
            // with the R operator's production behaviour.
            use_image: "Last".to_string(),
            pg_mode: "grid".to_string(),
            debug_show: 0,
            array_layout_file: files.layout_path.to_string_lossy().into_owned(),
            images_list: files
                .image_paths
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect(),
            gridding_output_file: String::new(),
        };

        tracing::info!(
            doc_id = %doc_id,
            n_images = group.images_list.len(),
            spot_pitch,
            rotation_n = group.rotation.len(),
            "running grid pipeline for chip group"
        );
        let started = std::time::Instant::now();
        let spots = process_single_group(&group)
            .map_err(|e| anyhow!("doc_id={doc_id} grid pipeline: {e}"))?;
        let elapsed = started.elapsed();
        tracing::info!(
            doc_id = %doc_id,
            n_spots = spots.len(),
            elapsed_ms = elapsed.as_millis(),
            "chip group done"
        );

        out.push(GroupResult {
            doc_id: doc_id.clone(),
            cis: files.cis.clone(),
            image_labels: files.image_labels.clone(),
            spots,
            spot_pitch,
        });
    }
    Ok(out)
}

fn autodetect_spot_pitch(first_image: &Path) -> Result<f64> {
    let img = load_tiff_image(first_image).context("load first image for pitch detect")?;
    let kind = ImageType::detect(img.width, img.height);
    kind.default_spot_pitch().ok_or_else(|| {
        anyhow!(
            "cannot auto-detect spot pitch from image dimensions {}×{} \
             ({:?}). Set the 'Spot Pitch' operator property explicitly.",
            img.width,
            img.height,
            kind
        )
    })
}
