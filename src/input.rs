//! Input table reading for the pamsoft_grid operator.
//!
//! The operator's input is a Tercen crosstab where:
//!
//!   * The **column factor** carries one or two `documentId`-typed
//!     factors — Tercen document IDs of the image ZIP (and optionally
//!     the layout file). Matches the R operator's contract.
//!   * The **y-axis projection** carries at least one label factor —
//!     the per-image **filename stem** (the `ctx$labels[[1]]` the R
//!     operator uses).
//!
//! Labels do NOT live on the column-facet table — `ctx$labels[[1]]` in
//! R is broadcast into the main data table (`qt_hash`), one value per
//! `(.ri, .ci)` cell. To get one label per crosstab column we stream
//! `.ci` + label from `qt_hash` and take the first non-null value seen
//! per `.ci`. We separately stream the column-facet (positional `.ci`,
//! row 0 → 0, row 1 → 1, …) for documentIds and join the two by `.ci`.
//!
//! Rows sharing a primary documentId belong to the same chip (same
//! ZIP), so we **group rows by their primary documentId** to form chip
//! groups for the algorithm. The original R operator groups by `.ci`;
//! grouping by documentId is the semantically equivalent reconstruction
//! given that we have to join across two tables anyway.

use anyhow::{anyhow, bail, Context, Result};
use polars::prelude::*;
use std::collections::BTreeMap;
use tercen_rs::context::ContextBase;
use tercen_rs::tson_to_dataframe;

/// One image-row of the operator's input table, decoded into native Rust types.
#[derive(Debug, Clone)]
pub struct InputRow {
    /// Implicit `.ci` for this row — the row index in the column-facet
    /// table (which equals the crosstab column index). Carried through
    /// so stage 6 (result table construction) can tag each output spot
    /// back to its source column.
    pub ci: i32,
    /// Filename stem of this image, taken from the first label factor.
    pub image_label: String,
    /// Document IDs referenced by this row (1 or 2 values, in the order
    /// the doc-ID columns appear in the schema). The first one is
    /// conventionally the image ZIP; the second, if present, is the
    /// array-layout text file. Same convention as the R operator's
    /// `prep_image_folder()` (`aux_functions.R:26-79`).
    pub document_ids: Vec<String>,
}

/// All rows from the input table, plus the schema introspection we
/// did along the way (doc-ID column names + label factor name). Rows
/// are grouped by their primary documentId — one chip = one image ZIP
/// doc-id — so iteration over `groups` walks chip-by-chip.
#[derive(Debug, Clone)]
pub struct InputData {
    /// primary documentId (the image ZIP) → ordered list of rows
    /// referencing that ZIP. Order within a group preserves the
    /// column-facet row order (so `.ci`s within a group are ascending).
    pub groups: BTreeMap<String, Vec<InputRow>>,
    /// Names of the documentId columns we found in the schema, in
    /// schema order. `len()` is always 1 or 2.
    pub document_id_columns: Vec<String>,
    /// Name of the label factor used for `image_label`.
    pub label_column: String,
}

impl InputData {
    /// Number of distinct chip groups (one Rust grid-pipeline invocation per).
    pub fn n_groups(&self) -> usize {
        self.groups.len()
    }

    /// Total number of image rows across all groups.
    pub fn n_rows(&self) -> usize {
        self.groups.values().map(|v| v.len()).sum()
    }
}

/// Load the operator's input by joining the column-facet table (for
/// documentIds, indexed positionally by `.ci`) with the main data table
/// (for labels, keyed explicitly by `.ci`).
///
/// Errors loudly (no fallbacks) when the input shape doesn't match the
/// R operator's contract — wrong number of doc-ID columns, missing
/// label factor, empty tables, etc.
///
/// Takes `&ContextBase` rather than the `TercenContext` trait so we can
/// call `ctx.streamer()` and `ctx.cnames()` (which live on the concrete
/// base, not on the abstract trait). Both `ProductionContext` and
/// `DevContext` `Deref<Target = ContextBase>`, so callers can pass
/// `&ctx` where `ctx` is either — Rust's deref coercion takes care
/// of the conversion.
pub async fn load_input_data(ctx: &ContextBase) -> Result<InputData> {
    // Identify the column-facet table — that's where the documentId
    // columns live.
    let col_table_id = ctx.cube_query().column_hash.clone();
    if col_table_id.is_empty() {
        bail!(
            "operator has no column-facet table (cube_query.column_hash is empty). \
             The pamsoft_grid_operator expects at least one column factor — the \
             documentId column carrying the image ZIP reference."
        );
    }

    // Schema introspection: enumerate column-facet column names and find
    // the documentId column(s) by name substring (matches the R operator's
    // `grepl("documentId", x)` heuristic at `main.R:190-192`).
    let all_cnames = ctx
        .cnames()
        .await
        .map_err(|e| anyhow!("fetch column-facet schema: {e}"))?;
    let document_id_columns: Vec<String> = all_cnames
        .iter()
        .filter(|c| c.contains("documentId"))
        .cloned()
        .collect();
    if document_id_columns.is_empty() || document_id_columns.len() > 2 {
        bail!(
            "expected 1 or 2 documentId columns on the column-facet table, found {} ({:?}). \
             Workflow-side: add a documentId-typed factor that references the image ZIP \
             (and optionally a second one for the array-layout file).",
            document_id_columns.len(),
            document_id_columns,
        );
    }

    // The label factor name comes from the first axis query's `labels`.
    // The R operator uses `ctx$labels[[1]]` (the first one) as the image
    // filename per row.
    let label_column = ctx
        .cube_query()
        .axis_queries
        .first()
        .and_then(|aq| aq.labels.first())
        .ok_or_else(|| {
            anyhow!(
                "no label factor on the input — the operator needs a label \
                 factor on the y-axis projection carrying each image's \
                 filename stem (matches the R operator's `ctx$labels[[1]]`)."
            )
        })?
        .name
        .clone();

    // --- Stream column-facet for documentIds (one row per .ci) ---
    let streamer = ctx.streamer();
    let col_tson = streamer
        .stream_tson(
            &col_table_id,
            Some(document_id_columns.clone()),
            0,
            -1,
        )
        .await
        .map_err(|e| anyhow!("stream column-facet table {col_table_id}: {e}"))?;
    let col_df = tson_to_dataframe(&col_tson).context("parse TSON column-facet payload")?;

    let doc_cols: Vec<Series> = document_id_columns
        .iter()
        .map(|name| {
            col_df
                .column(name)
                .map_err(|e| anyhow!("missing documentId column '{}': {}", name, e))
                .and_then(|s| s.cast(&DataType::String).context("cast doc id to string"))
                .map(|c| c.take_materialized_series())
        })
        .collect::<Result<Vec<_>>>()?;
    let doc_series: Vec<&StringChunked> = doc_cols
        .iter()
        .map(|s| s.str().context("documentId is not string"))
        .collect::<Result<Vec<_>>>()?;
    let n_cols = doc_series
        .first()
        .map(|s| s.len())
        .unwrap_or(0);
    if n_cols == 0 {
        bail!(
            "column-facet table is empty — no images to process. Check the \
             workflow's input step produces at least one row."
        );
    }

    // --- Stream main data table for `.ci` + label (broadcast per cell) ---
    // Pick the first non-null label per `.ci`. The label is constant
    // across `.ri` for a given `.ci` by construction (it's a column-level
    // attribute), so the first non-null seen is the answer.
    let qt_table_id = ctx.cube_query().qt_hash.clone();
    if qt_table_id.is_empty() {
        bail!(
            "operator has no main data table (cube_query.qt_hash is empty). \
             This shouldn't happen for a normal crosstab — re-check the workflow."
        );
    }
    let qt_tson = streamer
        .stream_tson(
            &qt_table_id,
            Some(vec![".ci".to_string(), label_column.clone()]),
            0,
            -1,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "stream main data table {qt_table_id} for .ci + label '{label_column}': {e}. \
                 Available main-table columns may be different; the label factor must be \
                 a label on axis_queries[0] and present on the main data."
            )
        })?;
    let qt_df = tson_to_dataframe(&qt_tson).context("parse TSON main-table payload")?;

    let ci_col = qt_df
        .column(".ci")
        .map_err(|e| anyhow!("missing .ci column on main table: {e}"))?
        .cast(&DataType::Int32)
        .context("cast .ci to int32")?;
    let label_col = qt_df
        .column(&label_column)
        .map_err(|e| anyhow!("missing label column '{}' on main table: {}", label_column, e))?
        .cast(&DataType::String)
        .context("cast label column to string")?;
    let ci_chunked = ci_col.i32().context(".ci is not int32")?;
    let label_chunked = label_col.str().context("label is not string")?;

    let mut ci_to_label: BTreeMap<i32, String> = BTreeMap::new();
    for (ci_opt, lbl_opt) in ci_chunked.into_iter().zip(label_chunked.into_iter()) {
        let (Some(ci), Some(lbl)) = (ci_opt, lbl_opt) else {
            continue;
        };
        ci_to_label.entry(ci).or_insert_with(|| lbl.to_string());
    }
    if ci_to_label.is_empty() {
        bail!(
            "main data table yielded zero (.ci, label) pairs — label column \
             '{label_column}' appears to be all-null on qt_hash. Check that the \
             label factor is wired into the workflow."
        );
    }

    // --- Join: for each column-facet row (positional .ci), pull its label ---
    let mut groups: BTreeMap<String, Vec<InputRow>> = BTreeMap::new();
    for row_idx in 0..n_cols {
        let ci = row_idx as i32;
        let image_label = ci_to_label
            .get(&ci)
            .ok_or_else(|| {
                anyhow!(
                    "no label found in main table for .ci={ci} (column-facet has \
                     {n_cols} rows but main table is missing this column). The \
                     workflow may have an empty column."
                )
            })?
            .clone();
        let document_ids: Vec<String> = doc_series
            .iter()
            .map(|s| {
                s.get(row_idx)
                    .ok_or_else(|| anyhow!("null documentId at .ci={ci}"))
                    .map(String::from)
            })
            .collect::<Result<Vec<_>>>()?;
        let group_key = document_ids[0].clone();
        groups.entry(group_key).or_default().push(InputRow {
            ci,
            image_label,
            document_ids,
        });
    }

    Ok(InputData {
        groups,
        document_id_columns,
        label_column,
    })
}
