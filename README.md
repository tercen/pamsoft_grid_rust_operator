# pamsoft_grid_rust_operator

Tercen operator that runs the [pamsoft_grid Rust port](https://github.com/tercen/pamsoft_grid_rust)
(peptide-microarray gridding + segmentation) on workflow image inputs.

A direct replacement for [`pamsoft_grid_operator`](https://github.com/pamgene/pamsoft_grid_operator) (the R/MATLAB-MCR
version), with the same input contract and column schema but no
MATLAB dependency at runtime — the algorithm is Rust calling
into OpenCV via the [`pamsoft_grid`](https://github.com/tercen/pamsoft_grid_rust) library crate.

## Input contract

The operator step's input must be a crosstab where the **column
factor** has:

- **1 or 2 `documentId`-typed columns** — the image ZIP, and
  optionally the array-layout `.txt` file. If there's only one
  documentId, the layout is expected to live inside the image ZIP.
- **At least one label factor** — the filename stem of each
  image (without the `.tif` extension). Each row of the column
  facet table is one image; rows sharing a `.ci` form a chip group
  that the algorithm processes together.

This matches the legacy R operator's input shape one-for-one.

## Properties

| Name | Type | Default | Description |
|---|---|---:|---|
| `Min Diameter` | Double | 0.45 | Lower bound for the segmented spot diameter (fraction of pitch). |
| `Max Diameter` | Double | 0.85 | Upper bound for the segmented spot diameter. |
| `Saturation Limit` | Double | 4095 | Saturation cutoff in raw pixel intensity. |
| `Spot Pitch` | Double | 0 | Distance between adjacent spots in pixels. `0` = auto-detect (Evolve2 = 21.5, Evolve3 = 17.0). |
| `Edge Sensitivity` | Double | 0.01 | Canny edge high threshold. |
| `Spot Size` | Double | 0.66 | Spot radius as a fraction of `Spot Pitch`. |
| `Segmentation Method` | Enum | `Edge` | `Edge` or `Hough`. |
| `Rotation` | String | `-2:0.25:2` | Rotation candidates in `min:step:max` syntax. Single value (`0`) triggers MATLAB's broken `imregister2` path — use a vector. |

## Local development

The operator's binary entry point is `src/main.rs`; the production
container at `pamgene/pamsoft_grid_rust_operator:<tag>` runs it with
`--taskId`, `--serviceUri`, `--token` injected by Tercen.

For local iteration against a real workflow without going through
Tercen's task-spawn machinery, the `dev` binary takes `WORKFLOW_ID`
and `STEP_ID` env vars and runs the same pipeline:

```bash
export TERCEN_URI=https://pamgene.tercen.com:443
export TERCEN_TOKEN=<your token>
export WORKFLOW_ID=<your workflow id>
export STEP_ID=<your step id>
cargo run --bin dev
```

## Build

System prerequisites (Debian/Ubuntu):

```
sudo apt install libopencv-dev libclang-dev clang pkg-config protobuf-compiler
```

Then:

```
cargo build --release --bin pamsoft_grid_operator
```

The `pamsoft_grid` library dep brings in OpenCV bindings — first
build takes ~10 minutes, incremental rebuilds are seconds.

## CI / release

- **Every push to `main`** → `ci.yml` builds the Docker image and
  pushes `pamgene/pamsoft_grid_rust_operator:<commit-sha>` and
  `:latest`.
- **Pushing a semver tag** (`0.1.0`, `0.2.0`, …) → `release.yml`
  rewrites `operator.json.container` to the tagged image, builds,
  pushes `:<tag>` and `:latest`, and creates a GitHub Release.

Install in Tercen via the UI or:

```
tercenctl operator install --repo https://github.com/tercen/pamsoft_grid_rust_operator --tag <version>
```

## Architecture

```
Tercen task invocation
       │
       ▼
src/main.rs ─── parse --taskId/--serviceUri/--token → env vars
       │
       ▼
src/lib.rs::run  →  ProductionContext::from_task_id
       │            (or DevContext::from_workflow_step for the dev binary)
       ▼
src/lib.rs::execute
   │
   ├── src/props.rs        — read operator properties
   ├── src/input.rs        — stream the column-facet table
   ├── src/download.rs     — fetch + extract documentId ZIPs
   └── src/algorithm.rs    — pamsoft_grid::batch::process_single_group
                             (currently no result upload; stages 6-7 pending)
```
