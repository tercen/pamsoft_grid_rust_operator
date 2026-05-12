//! Input table reading for the pamsoft_grid operator.
//!
//! The operator's input is a Tercen crosstab where each column facet
//! represents one image file in a chip group:
//!
//!   * `.ci` — the column-facet index, used to **group images into chips**.
//!     One unique `.ci` per chip; multiple rows per `.ci` for multi-image
//!     chips. Mirrors the R operator's `groups <- unique(df$.ci)` step.
//!   * One or more `documentId`-typed columns on the column-facet table,
//!     each holding the Tercen document ID of the image ZIP (and
//!     optionally the layout file). The R operator allows 1 or 2 doc-ID
//!     columns (`main.R:190-194`); we follow the same rule.
//!   * One label factor (`ctx.labels()[0]`) — the per-row image
//!     **filename stem**. Combined with the doc-ID column's path, this
//!     identifies which TIFF inside the ZIP each row references.
//!
//! `load_input_data` streams the column-facet table once via the
//! tercen-rs `TableStreamer`, parses the TSON payload into a Polars
//! DataFrame, and groups the rows by `.ci`. The returned
//! [`InputData`] structure is what stage 4 (file download) consumes.

use anyhow::{anyhow, bail, Context, Result};
use polars::prelude::*;
use std::collections::BTreeMap;
use tercen_rs::context::ContextBase;
use tercen_rs::tson_to_dataframe;

/// One image-row of the operator's input table, decoded into native Rust types.
#[derive(Debug, Clone)]
pub struct InputRow {
    /// Column-facet index — used as the grouping key (chip = one unique `.ci`).
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
/// did along the way (doc-ID column names + label factor name). The
/// rows are pre-grouped by `.ci`; iteration order is `.ci`-ascending.
#[derive(Debug, Clone)]
pub struct InputData {
    /// `.ci` → ordered list of rows with that `.ci`.
    pub groups: BTreeMap<i32, Vec<InputRow>>,
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

/// Stream the column-facet table and decode it into [`InputData`].
///
/// Errors loudly (no fallbacks) when the input shape doesn't match the
/// R operator's contract — wrong number of doc-ID columns, missing
/// label factor, empty column-facet table, etc.
///
/// Takes `&ContextBase` rather than the `TercenContext` trait so we can
/// call `ctx.streamer()` and `ctx.cnames()` (which live on the concrete
/// base, not on the abstract trait). Both `ProductionContext` and
/// `DevContext` `Deref<Target = ContextBase>`, so callers can pass
/// `&ctx` where `ctx` is either — Rust's deref coercion takes care
/// of the conversion.
pub async fn load_input_data(ctx: &ContextBase) -> Result<InputData> {
    // Identify the column-facet table — that's where the documentId
    // columns and the label factor live.
    let table_id = ctx.cube_query().column_hash.clone();
    if table_id.is_empty() {
        bail!(
            "operator has no column-facet table (cube_query.column_hash is empty). \
             The pamsoft_grid_operator expects at least one column factor — the \
             documentId column carrying the image ZIP reference."
        );
    }

    // Schema introspection: enumerate column names and find the documentId
    // column(s) by name substring (matches the R operator's `grepl("documentId", x)`
    // heuristic at `main.R:190-192`).
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

    // The label factor names come from the first axis query's `labels`.
    // The R operator uses `ctx$labels[[1]]` (the first one) as the image
    // filename per row. Re-implementing the trait's default `labels()`
    // inline because `ContextBase` itself doesn't impl `TercenContext` —
    // only its wrappers do, and we'd rather take the concrete base type
    // to keep `cnames()` / `streamer()` reachable.
    let label_column = ctx
        .cube_query()
        .axis_queries
        .first()
        .and_then(|aq| aq.labels.first())
        .ok_or_else(|| {
            anyhow!(
                "no label factor on the input — the operator needs a label \
                 factor carrying each image's filename stem (matches the R \
                 operator's `ctx$labels[[1]]`)."
            )
        })?
        .name
        .clone();

    // Stream the whole column-facet table, restricted to the columns
    // we actually need. `.ci` is implicit (tercen tables carry it
    // automatically); doc-ID + label are explicit.
    let mut cols = vec![".ci".to_string(), label_column.clone()];
    cols.extend(document_id_columns.iter().cloned());

    let streamer = ctx.streamer();
    let tson = streamer
        .stream_tson(&table_id, Some(cols.clone()), 0, -1)
        .await
        .map_err(|e| anyhow!("stream column-facet table {table_id}: {e}"))?;
    let df = tson_to_dataframe(&tson).context("parse TSON column-facet payload")?;

    // Pull the columns we need out of the DataFrame.
    let ci_col = df
        .column(".ci")
        .map_err(|e| anyhow!("missing '.ci' column in column-facet TSON: {e}"))?
        .cast(&DataType::Int32)
        .context("cast .ci to i32")?;
    let label_col = df
        .column(&label_column)
        .map_err(|e| anyhow!("missing label column '{}': {}", label_column, e))?
        .cast(&DataType::String)
        .context("cast label column to string")?;
    let doc_cols: Vec<Series> = document_id_columns
        .iter()
        .map(|name| {
            df.column(name)
                .map_err(|e| anyhow!("missing documentId column '{}': {}", name, e))
                .and_then(|s| s.cast(&DataType::String).context("cast doc id to string"))
                .map(|c| c.take_materialized_series())
        })
        .collect::<Result<Vec<_>>>()?;

    let ci_series = ci_col.i32().context("ci is not i32")?;
    let label_series = label_col.str().context("label is not string")?;
    let doc_series: Vec<&StringChunked> = doc_cols
        .iter()
        .map(|s| s.str().context("documentId is not string"))
        .collect::<Result<Vec<_>>>()?;

    let n = ci_series.len();
    if n == 0 {
        bail!(
            "column-facet table is empty — no images to process. Check the \
             workflow's input step produces at least one row."
        );
    }
    let mut groups: BTreeMap<i32, Vec<InputRow>> = BTreeMap::new();
    for row_idx in 0..n {
        let ci = ci_series
            .get(row_idx)
            .ok_or_else(|| anyhow!("null .ci at row {row_idx}"))?;
        let image_label = label_series
            .get(row_idx)
            .ok_or_else(|| anyhow!("null label at row {row_idx}"))?
            .to_string();
        let document_ids: Vec<String> = doc_series
            .iter()
            .map(|s| {
                s.get(row_idx)
                    .ok_or_else(|| anyhow!("null documentId at row {row_idx}"))
                    .map(String::from)
            })
            .collect::<Result<Vec<_>>>()?;
        groups.entry(ci).or_default().push(InputRow {
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
