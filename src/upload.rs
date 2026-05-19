//! Stage 7: result-table upload.
//!
//! Production path: fetch the `ETask` via `TaskService.get(task_id)`,
//! pass it (mutably) to `ContextBase::save_table(&df, &mut task)`.
//! tercen-rs handles TSON encoding, file upload, and the
//! `fileResultId` field on the task.
//!
//! Dev path: skipped — `DevContext` has no task. The dev binary logs
//! the row count and returns Ok, which lets the rest of the pipeline
//! be exercised end-to-end against a real workflow without writing
//! anything back.

use anyhow::{anyhow, Result};
use polars::frame::DataFrame;
use tercen_rs::client::proto::GetRequest;
use tercen_rs::context::ContextBase;
use tonic::Request;

/// Upload the result DataFrame back to Tercen. Fetches the task
/// first (`save_table` mutates it to attach the `fileResultId`).
pub async fn save_results(ctx: &ContextBase, task_id: &str, df: &DataFrame) -> Result<()> {
    use tercen_rs::client::proto::e_task;

    let mut task_service = ctx
        .client()
        .task_service()
        .map_err(|e| anyhow!("acquire task service: {e}"))?;
    let task = task_service
        .get(Request::new(GetRequest {
            id: task_id.to_string(),
            ..Default::default()
        }))
        .await
        .map_err(|e| anyhow!("fetch task {task_id} for save_table: {e}"))?
        .into_inner();
    // Sanity check: `save_table` mutates the task in place; reject task
    // shapes the SDK doesn't know how to update so the failure is loud
    // rather than a silent no-op on the server.
    match task.object.as_ref() {
        Some(e_task::Object::Computationtask(_)) | Some(e_task::Object::Runcomputationtask(_)) => {}
        Some(other) => {
            return Err(anyhow!(
                "task {task_id} has unexpected type {:?} — save_table only supports \
                 ComputationTask / RunComputationTask",
                std::mem::discriminant(other),
            ));
        }
        None => return Err(anyhow!("task {task_id} has no `object` field")),
    }
    let mut task = task;

    tracing::info!(
        n_rows = df.height(),
        n_cols = df.width(),
        task_id,
        "uploading result table"
    );
    ctx.save_table(df, &mut task)
        .await
        .map_err(|e| anyhow!("save_table: {e}"))?;
    tracing::info!("result table uploaded");
    Ok(())
}
