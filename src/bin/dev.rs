//! Dev binary: no CLI args, reads `TERCEN_URI`, `TERCEN_TOKEN`,
//! `WORKFLOW_ID`, and `STEP_ID` straight from the environment. Use this
//! to run the operator against a Tercen instance without going through
//! the platform's task-spawning machinery — points at a workflow/step
//! you're authoring and runs the operator pipeline locally.
//!
//! Example (against the public Tercen Stage):
//! ```bash
//! export TERCEN_URI=https://tercen.com:443
//! export TERCEN_TOKEN=<your token>
//! export WORKFLOW_ID=<your workflow id>
//! export STEP_ID=<your step id within that workflow>
//! cargo run --bin dev
//! ```
//!
//! Logs go to stdout (same `tracing` formatting as the production
//! binary's task logs). No result is uploaded — at this milestone the
//! operator only logs the properties + input-table shape it sees.

use anyhow::Result;
use pamsoft_grid_operator::{init_tracing, require_env, run_dev};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    for var in ["TERCEN_URI", "TERCEN_TOKEN", "WORKFLOW_ID", "STEP_ID"] {
        require_env(var)?;
    }

    let workflow_id = require_env("WORKFLOW_ID")?;
    let step_id = require_env("STEP_ID")?;
    run_dev(&workflow_id, &step_id).await
}
