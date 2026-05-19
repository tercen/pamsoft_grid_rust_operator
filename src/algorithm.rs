//! Stage 5: algorithm invocation.
//!
//! For each `InputRow` (one chip = one `.ci` = one image, matching R),
//! build a `pamsoft_grid::types::GroupConfig` and call
//! `pamsoft_grid::batch::process_single_group`, returning the per-spot
//! `SpotResult` rows. This is the bit that replaces "shell out to MATLAB
//! MCR" in the original R operator.
//!
//! Multi-image chips (multiple `.ci`s sharing a documentId) get one
//! gridding pass each — the same way R does it via
//! `group_by(.ci) %>% group_walk(prep_grid_files)`.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use pamsoft_grid::batch::process_single_group;
use pamsoft_grid::io::load_tiff_image;
use pamsoft_grid::types::{GroupConfig, ImageType, SpotResult};

use crate::download::DocumentCatalogue;
use crate::input::{InputData, InputRow};
use crate::props::PamsoftProps;

/// One chip's grid-detection output, tagged with its source `.ci` and
/// image filename so stage 6 can emit the result rows.
pub struct GroupResult {
    pub ci: i32,
    pub image_label: String,
    pub spots: Vec<SpotResult>,
    /// Effective spot pitch used (after auto-detection if the user left
    /// `Spot Pitch = 0`). Surfaced for logging / debugging.
    pub spot_pitch: f64,
}

/// Run the grid algorithm on every input row (one chip per `.ci`).
/// Returns results in the input's `.ci` order.
pub fn run_grid_per_group(
    input: &InputData,
    catalogue: &DocumentCatalogue,
    layout_path: &Path,
    props: &PamsoftProps,
) -> Result<Vec<GroupResult>> {
    let mut out = Vec::with_capacity(input.rows.len());
    for row in &input.rows {
        let tiff_path = resolve_tiff(row, catalogue)?;

        let spot_pitch = if props.spot_pitch > 0.0 {
            props.spot_pitch
        } else {
            // Auto-detect from the image's dimensions, matching the R
            // operator's `get_imageset_type` fallback (552×413 → 17.0
            // Evolve3, 697×520 → 21.5 Evolve2; anything else errors).
            autodetect_spot_pitch(&tiff_path)
                .with_context(|| format!(".ci={}: auto-detect spot pitch", row.ci))?
        };

        let group = GroupConfig {
            group_id: format!("ci_{}", row.ci),
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
            // R operator uses "Last" but with only one image per chip
            // First and Last are equivalent. Keep "Last" for parity.
            use_image: "Last".to_string(),
            pg_mode: "grid".to_string(),
            debug_show: 0,
            array_layout_file: layout_path.to_string_lossy().into_owned(),
            images_list: vec![tiff_path.to_string_lossy().into_owned()],
            gridding_output_file: String::new(),
        };

        tracing::info!(
            ci = row.ci,
            image = %row.image_label,
            spot_pitch,
            rotation_n = group.rotation.len(),
            "running grid pipeline for chip"
        );
        let started = std::time::Instant::now();
        let spots = process_single_group(&group)
            .map_err(|e| anyhow!(".ci={} ('{}') grid pipeline: {}", row.ci, row.image_label, e))?;
        let elapsed = started.elapsed();
        tracing::info!(
            ci = row.ci,
            image = %row.image_label,
            n_spots = spots.len(),
            elapsed_ms = elapsed.as_millis(),
            "chip done"
        );

        out.push(GroupResult {
            ci: row.ci,
            image_label: row.image_label.clone(),
            spots,
            spot_pitch,
        });
    }
    Ok(out)
}

/// Resolve the on-disk TIFF for an input row by looking up the row's
/// image_label in the primary documentId's TIFF index.
fn resolve_tiff(row: &InputRow, catalogue: &DocumentCatalogue) -> Result<std::path::PathBuf> {
    let primary = row
        .document_ids
        .first()
        .ok_or_else(|| anyhow!(".ci={} has no documentId", row.ci))?;
    let doc = catalogue
        .get(primary)
        .ok_or_else(|| anyhow!(".ci={}: documentId {} not in catalogue", row.ci, primary))?;
    doc.tiff_index
        .get(&row.image_label)
        .cloned()
        .ok_or_else(|| {
            let sample = doc
                .tiff_index
                .keys()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            anyhow!(
                ".ci={} ('{}'): no TIFF with stem '{}' in documentId {}. \
                 Available stems (first 5): {}",
                row.ci,
                row.image_label,
                row.image_label,
                primary,
                sample,
            )
        })
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
