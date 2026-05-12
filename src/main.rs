//! Production binary: invoked by Tercen with `--taskId`, `--serviceUri`,
//! `--token`. We translate those into `TERCEN_TASK_ID` / `TERCEN_URI` /
//! `TERCEN_TOKEN` and delegate to [`pamsoft_grid_operator::run`], which
//! is shared with the dev binary.

use anyhow::Result;
use pamsoft_grid_operator::{init_tracing, require_env, run};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    parse_args_into_env(&args);

    let task_id = require_env("TERCEN_TASK_ID")?;
    run(&task_id).await
}

/// Tercen invokes operators with `--taskId X --serviceUri Y --token Z`.
/// Rewrite those into the environment so `TercenClient::from_env` works.
fn parse_args_into_env(args: &[String]) {
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--taskId" if i + 1 < args.len() => {
                std::env::set_var("TERCEN_TASK_ID", &args[i + 1]);
                i += 2;
            }
            "--serviceUri" if i + 1 < args.len() => {
                std::env::set_var("TERCEN_URI", &args[i + 1]);
                i += 2;
            }
            "--token" if i + 1 < args.len() => {
                std::env::set_var("TERCEN_TOKEN", &args[i + 1]);
                i += 2;
            }
            _ => i += 1,
        }
    }
}
