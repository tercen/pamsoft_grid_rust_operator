//! pamsoft_grid_operator — Tercen operator for peptide microarray gridding.
//!
//! The crate exposes a single entry point, [`run`], shared between the
//! production binary (`src/main.rs`, invoked by Tercen with `--taskId` /
//! `--serviceUri` / `--token`) and the dev binary (`src/bin/dev.rs`, driven
//! by `TERCEN_*` environment variables).
//!
//! At this stage the function only connects to Tercen and prints task
//! metadata — input-data download, algorithm execution and result upload
//! land in later chunks.

pub mod algorithm;
pub mod download;
pub mod input;
pub mod output;
pub mod props;
pub mod upload;

use std::sync::Arc;

use anyhow::{Context, Result};
use tercen_rs::context::ContextBase;
use tercen_rs::{DevContext, ProductionContext, TercenClient};

/// Production entry point. Bootstraps a `ProductionContext` from a
/// task ID (the Tercen platform passes this in via `--taskId`), then
/// hands off to [`execute`] for the actual operator pipeline.
///
/// `TERCEN_URI` and `TERCEN_TOKEN` must already be in the environment —
/// the production binary (`src/main.rs`) extracts them from CLI args
/// before calling this; the dev binary doesn't go through this function.
pub async fn run(task_id: &str) -> Result<()> {
    tracing::info!("pamsoft_grid_operator starting (task_id={task_id})");
    let client = build_client().await?;
    let ctx = ProductionContext::from_task_id(client, task_id)
        .await
        .map_err(|e| anyhow::anyhow!("load task {task_id}: {e}"))?;
    execute(&ctx, Some(task_id)).await
}

/// Dev entry point. Bootstraps a `DevContext` from a workflow/step
/// pair (no Tercen-side task needed — useful for running the operator
/// locally against a workflow you're authoring), then hands off to
/// [`execute`].
///
/// Same `TERCEN_URI` / `TERCEN_TOKEN` requirement as [`run`].
pub async fn run_dev(workflow_id: &str, step_id: &str) -> Result<()> {
    tracing::info!(
        "pamsoft_grid_operator starting in dev mode \
         (workflow_id={workflow_id}, step_id={step_id})"
    );
    let client = build_client().await?;
    let ctx = DevContext::from_workflow_step(client, workflow_id, step_id)
        .await
        .map_err(|e| anyhow::anyhow!("load workflow {workflow_id} / step {step_id}: {e}"))?;
    execute(&ctx, None).await
}

async fn build_client() -> Result<Arc<TercenClient>> {
    let client = TercenClient::from_env()
        .await
        .map_err(|e| anyhow::anyhow!("connect to Tercen: {e}"))?;
    tracing::info!("connected to Tercen");
    Ok(Arc::new(client))
}

/// Pipeline implementation, generic over the context flavour (taking
/// the concrete `&ContextBase` since `ProductionContext` and
/// `DevContext` both `Deref<Target = ContextBase>`).
///
/// `task_id` is `Some(...)` in production mode (so we can fetch the
/// `ETask` and upload results via `save_table`) and `None` in dev mode
/// (no task — we just log the result row count and return).
async fn execute(ctx: &ContextBase, task_id: Option<&str>) -> Result<()> {
    tracing::info!(
        workflow = ctx.workflow_id(),
        step = ctx.step_id(),
        project = ctx.project_id(),
        namespace = ctx.namespace(),
        "context loaded"
    );

    let pamsoft_props = props::read_pamsoft_props(ctx.operator_settings())
        .map_err(|e| anyhow::anyhow!("read operator properties: {e}"))?;
    tracing::info!(
        min_diameter = pamsoft_props.min_diameter,
        max_diameter = pamsoft_props.max_diameter,
        spot_pitch = pamsoft_props.spot_pitch,
        spot_size = pamsoft_props.spot_size,
        rotation_n = pamsoft_props.rotation.len(),
        saturation_limit = pamsoft_props.saturation_limit,
        edge_high = pamsoft_props.edge_sensitivity[1],
        seg_method = pamsoft_props.seg_method,
        "operator properties parsed"
    );

    let input_data = input::load_input_data(ctx)
        .await
        .map_err(|e| anyhow::anyhow!("load input table: {e:#}"))?;
    tracing::info!(
        n_chips = input_data.n_groups(),
        doc_id_cols = ?input_data.document_id_columns,
        label_col = input_data.label_column,
        "input table loaded (one chip per .ci)"
    );

    // Stage 4: download every unique documentId once and index TIFFs by
    // filename stem. Temp dir is task-scoped — Tercen task containers
    // are ephemeral so cleanup happens on container exit; we still
    // RemoveOnDrop it in case the operator is rerun in the same
    // container (dev mode).
    let work_dir_key = ctx.workflow_id().to_string() + "_" + ctx.step_id();
    let work_root = std::env::temp_dir().join(format!("pamsoft_op_{}", work_dir_key));
    let _drop_guard = TempDirGuard(work_root.clone());
    let (catalogue, layout_path) = download::download_all(ctx, &input_data, &work_root)
        .await
        .map_err(|e| anyhow::anyhow!("file download: {e:#}"))?;
    tracing::info!(
        n_docs = catalogue.len(),
        layout = %layout_path.display(),
        work_root = %work_root.display(),
        "input files ready on disk"
    );

    // Stage 5: run the grid algorithm per .ci (one chip = one image).
    let group_results =
        algorithm::run_grid_per_group(&input_data, &catalogue, &layout_path, &pamsoft_props)
            .map_err(|e| anyhow::anyhow!("grid algorithm: {e:#}"))?;
    let total_spots: usize = group_results.iter().map(|g| g.spots.len()).sum();
    tracing::info!(
        n_groups = group_results.len(),
        total_spots,
        "grid algorithm complete"
    );

    // Stage 6: build the Polars result DataFrame.
    let df = output::build_result_df(&group_results, ctx.namespace())
        .map_err(|e| anyhow::anyhow!("build result DataFrame: {e:#}"))?;
    tracing::info!(
        n_rows = df.height(),
        n_cols = df.width(),
        namespace = ctx.namespace(),
        "result DataFrame built"
    );

    // Stage 7: upload (production) or log + skip (dev).
    match task_id {
        Some(tid) => {
            upload::save_results(ctx, tid, &df)
                .await
                .map_err(|e| anyhow::anyhow!("upload result table: {e:#}"))?;
        }
        None => {
            // In dev mode, dump the result DataFrame to a local CSV when
            // OUTPUT_CSV is set. Used by the e2e test driver to diff
            // against the R operator's Tercen-side output without going
            // through the Docker-build / save_table round-trip.
            if let Ok(path) = std::env::var("OUTPUT_CSV") {
                write_csv(&df, &path)
                    .map_err(|e| anyhow::anyhow!("dump result CSV to {path}: {e:#}"))?;
                tracing::info!(
                    n_rows = df.height(),
                    path,
                    "dev mode: result DataFrame written to CSV"
                );
            } else {
                tracing::info!(
                    n_rows = df.height(),
                    "dev mode: skipping save_table (no task — DevContext has no ETask to \
                     mutate). Set OUTPUT_CSV=<path> to dump the result DataFrame locally."
                );
            }
        }
    }

    Ok(())
}

/// Dump a DataFrame to a CSV. Used only in dev mode + only when
/// `OUTPUT_CSV` is set, so the I/O dep stays out of the production hot
/// path. Format matches Polars' default `CsvWriter` (RFC 4180-ish, with
/// the header row).
fn write_csv(df: &polars::frame::DataFrame, path: &str) -> Result<()> {
    use polars::prelude::*;
    let mut df = df.clone();
    let mut f = std::fs::File::create(path)
        .with_context(|| format!("create CSV at {path}"))?;
    CsvWriter::new(&mut f)
        .include_header(true)
        .finish(&mut df)
        .map_err(|e| anyhow::anyhow!("CsvWriter: {e}"))?;
    Ok(())
}

/// Best-effort temp-dir cleanup on `run` / `run_dev` exit. Tercen tasks
/// run in ephemeral containers so a leak here is harmless, but in dev
/// mode the same WSL machine sees many runs — clean up so /tmp doesn't
/// balloon.
struct TempDirGuard(std::path::PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Initialise tracing once — called by both binaries.
pub fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();
}

/// Require an environment variable to be set; return a helpful error otherwise.
pub fn require_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} environment variable not set"))
}
