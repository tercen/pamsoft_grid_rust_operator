//! Stage 6: build the result Polars DataFrame from `Vec<GroupResult>`.
//!
//! Schema mirrors the R operator's `outFrame` in `main.R::do.grid`:
//!
//!   * `.ci`  (int32) — the chip's `.ci`. One chip = one `.ci` = one
//!     image, so every spot row for this chip carries the same `.ci`.
//!   * `.ri`  (int32) — spot index 0..nGrid-1 within the chip. R relies
//!     on `ctx$save()` to assign `.ri`, but tercen-rs' `save_table` is
//!     literal — emit it explicitly so the relation aligns with the
//!     y-axis (one row per spot).
//!   * `{ns}.IsReference` (string, "TRUE"/"FALSE") — `as.character(as.logical(...))`
//!   * `{ns}.ID` (string) — `qntSpotID`
//!   * `{ns}.spotRow`, `.spotCol` (f64) — `grdRow`, `grdCol`
//!   * `{ns}.grdXFixedPosition`, `.grdYFixedPosition`, `.gridX`, `.gridY`,
//!     `.diameter`, `.grdRotation` (f64)
//!   * `{ns}.manual`, `.bad`, `.empty` (f64) — bools as 0.0 / 1.0
//!     (matches R's `as.double(as.logical(...))`)
//!   * `{ns}.grdImageNameUsed` (string)
//!
//! Sorted by `(.ci, .ri)` to match R's `arrange(.ci)`.

use anyhow::{anyhow, Result};
use polars::prelude::*;

use crate::algorithm::GroupResult;

/// Build the result DataFrame for `save_table`. `namespace` is
/// `ctx.namespace()` (e.g. `"ds1"`); all non-`.ci`/`.ri` columns get
/// `"{namespace}."` prefixed onto their names.
pub fn build_result_df(groups: &[GroupResult], namespace: &str) -> Result<DataFrame> {
    let total: usize = groups.iter().map(|g| g.spots.len()).sum();

    let mut ci_vec: Vec<i32> = Vec::with_capacity(total);
    let mut ri_vec: Vec<i32> = Vec::with_capacity(total);
    let mut is_ref: Vec<String> = Vec::with_capacity(total);
    let mut id_vec: Vec<String> = Vec::with_capacity(total);
    let mut row_vec: Vec<f64> = Vec::with_capacity(total);
    let mut col_vec: Vec<f64> = Vec::with_capacity(total);
    let mut x_fixed: Vec<f64> = Vec::with_capacity(total);
    let mut y_fixed: Vec<f64> = Vec::with_capacity(total);
    let mut grid_x: Vec<f64> = Vec::with_capacity(total);
    let mut grid_y: Vec<f64> = Vec::with_capacity(total);
    let mut diameter: Vec<f64> = Vec::with_capacity(total);
    let mut manual: Vec<f64> = Vec::with_capacity(total);
    let mut bad: Vec<f64> = Vec::with_capacity(total);
    let mut empty: Vec<f64> = Vec::with_capacity(total);
    let mut rotation: Vec<f64> = Vec::with_capacity(total);
    let mut image_name_vec: Vec<String> = Vec::with_capacity(total);

    for g in groups {
        for (ri, s) in g.spots.iter().enumerate() {
            ci_vec.push(g.ci);
            ri_vec.push(ri as i32);
            is_ref.push(if s.is_reference { "TRUE" } else { "FALSE" }.to_string());
            id_vec.push(s.spot_id.clone());
            row_vec.push(s.row);
            col_vec.push(s.col);
            x_fixed.push(s.x_fixed);
            y_fixed.push(s.y_fixed);
            grid_x.push(s.grid_x);
            grid_y.push(s.grid_y);
            diameter.push(s.diameter);
            // R coerces these via `as.double(as.logical(...))` — anything
            // non-zero in MATLAB's int flags is logical TRUE → 1.0.
            manual.push(if s.is_manual != 0 { 1.0 } else { 0.0 });
            bad.push(if s.is_bad != 0 { 1.0 } else { 0.0 });
            empty.push(if s.is_empty != 0 { 1.0 } else { 0.0 });
            rotation.push(s.rotation);
            image_name_vec.push(s.image_name.clone());
        }
    }

    let ns_col = |suffix: &str| format!("{}.{}", namespace, suffix);

    let df = df! {
        ".ci" => ci_vec,
        ".ri" => ri_vec,
        &ns_col("IsReference") => is_ref,
        &ns_col("ID") => id_vec,
        &ns_col("spotRow") => row_vec,
        &ns_col("spotCol") => col_vec,
        &ns_col("grdXFixedPosition") => x_fixed,
        &ns_col("grdYFixedPosition") => y_fixed,
        &ns_col("gridX") => grid_x,
        &ns_col("gridY") => grid_y,
        &ns_col("diameter") => diameter,
        &ns_col("manual") => manual,
        &ns_col("bad") => bad,
        &ns_col("empty") => empty,
        &ns_col("grdRotation") => rotation,
        &ns_col("grdImageNameUsed") => image_name_vec,
    }
    .map_err(|e| anyhow!("build result DataFrame: {e}"))?;

    // Sort by `.ci`, then `.ri` — matches R's `arrange(.ci)` and keeps
    // spot ordering within a chip stable.
    let sorted = df
        .lazy()
        .sort(
            [".ci", ".ri"],
            SortMultipleOptions::default(),
        )
        .collect()
        .map_err(|e| anyhow!("sort result DataFrame: {e}"))?;

    Ok(sorted)
}
