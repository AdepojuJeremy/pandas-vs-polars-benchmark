# Pandas vs Polars (Rust) — High-Performance ETL on 12.7M NYC Taxi Rows

This repository compares an identical ETL workflow implemented in:

* **Python + Pandas** (eager, single-process)
* **Rust + Polars** (lazy, multi-threaded, SIMD-accelerated)

It uses the public **NYC Yellow Taxi (January 2015)** dataset (\~12.7M rows) to showcase where Rust + Polars can drastically cut wall-clock time and memory for production-style data jobs.

The Rust project also exposes a small **Axum HTTP API** (health/info/demo endpoints) suitable for deployment on **Shuttle**.

---

## Contents

* `python-pandas/` – Pandas ETL script
* `rust-polars/`

  * `src/etl.rs` – Polars ETL (lazy pipeline)
  * `src/main.rs` – CLI entrypoint and Axum service (Shuttle)
  * `Cargo.toml`, `Cargo.lock`, `Shuttle.toml`
  * `deploy_to_shuttle.sh` – helper script
* `data/` – (you place the CSV here; not committed)
* `results/` – metrics written by the ETL runs
* `run_benchmarks.sh` – optional wrapper to run both sides

---

## Dataset

* **Required file (not included):** `data/yellow_tripdata_2015-01.csv`
* Obtain from the **NYC TLC** open data site (NYC Yellow Taxi, January 2015).
* File size is \~2 GB (CSV). Expect **>8 GB RAM** recommended for Pandas runs.

> **Folder layout requirement**
>
> The Rust binary expects the CSV at: `../data/yellow_tripdata_2015-01.csv` when run from `rust-polars/`.
> The Pandas script expects: `../data/yellow_tripdata_2015-01.csv` when run from `python-pandas/`.

---

## Prerequisites

### Rust / Polars (Rust)

* Rust toolchain (stable)
* `cargo` installed
* CPU with multiple cores (Polars uses threads by default)

### Python / Pandas

* Python 3.10+ (recommended)
* A virtual environment (venv or conda)
* `pandas` and common scientific stack

> **Performance note**
> Always run Rust with **release** builds: `cargo run --release`. Debug builds are dramatically slower.

---

## Quick Start

### 1) Prepare the dataset

```
mkdir -p data results
# Place yellow_tripdata_2015-01.csv into ./data
```

### 2) Run the Pandas benchmark

```
cd python-pandas
python -u pandas_etl.py
```

**Representative output (example):**

```
Loaded 12,748,986 rows in 32.50s
Cleaned in 11.10s
Aggregated in 9.21s
Sorted/filtered in 9.42s
Total time: ~62.96s
Peak memory: ~4.6 GB
```

### 3) Run the Polars (Rust) benchmark

```
cd rust-polars
cargo run --release
```

**Representative output (example):**

```
🚀 STARTING POLARS ETL BENCHMARK
Aggregations: ~13.8s
Sort & filter: ~5.3s
Total time: ~19.1s
```

**Metrics output:** `results/polars_metrics.json`

> **Reproducibility**
> You can cap threads to stabilize timings:
>
> ```
> RAYON_NUM_THREADS=8 cargo run --release
> ```

---

## What the ETL does (both implementations)

1. **Load** selected columns from CSV
2. **Clean**

   * Drop zero lat/longs
   * Keep `0 < trip_distance < 100`
   * Keep `0 < passenger_count <= 6`
   * Parse timestamps; compute `trip_duration_minutes`
   * Keep `0 < trip_duration_minutes < 480`
3. **Aggregate** daily/hourly/weekday statistics
4. **Sort & Filter** (derive counts for long/expensive/rush-hour/weekend/premium trips)
5. **Save** metrics to `results/`

**Polars specifics:**

* Uses **LazyFrame**; filters and projections are pushed down
* Branches are **collected** at the aggregation step(s)
* Integer casts are used in counts to avoid `u32`/`i64` mismatches

---

## Results Snapshot (illustrative)

| Step          | Pandas (s) | Polars (s) | Notes                                           |
| ------------- | ---------- | ---------- | ----------------------------------------------- |
| Load          | \~32.5     | \~0.0\*    | \*Lazy scan creation is near-zero time          |
| Clean         | \~11.1     | \~0.0\*    | \*Planning happens; work is fused later         |
| Aggregations  | \~9.2      | \~13.8     | Polars collects & computes here                 |
| Sort & Filter | \~9.4      | \~5.3      | Polars fuses/streams with predicate pushdown    |
| **Total**     | **\~63.0** | **\~19.1** | Hardware/cores/IO matter; your numbers may vary |

> **Fairness checklist**
>
> * Same filters/caps and timestamp parsing
> * Rust was built with `--release`
> * Logging CPU/memory and environment
> * End-to-end timing reported

---

## Running the Axum API Locally

> The **API returns representative/demo metrics** (fast) and does **not** read the entire CSV in Shuttle or local API mode by default. The CLI ETL is the heavy run.

From `rust-polars/`:

```
cargo run --release
# If your CLI ETL runs as main, open a new terminal to run the server binary separately,
# or split the modes in code if you prefer.
```

**Endpoints (example):**

* `GET /health` – service status
* `GET /info` – dataset & comparison info
* `GET /benchmark` – returns demo metrics payload

**Sample:**

```
curl -s https://<your-app>.shuttle.app/health
curl -s https://<your-app>.shuttle.app/info
curl -s https://<your-app>.shuttle.app/benchmark
```

---

## Deploy to Shuttle

1. Install & login:

```
cargo install cargo-shuttle
cargo shuttle login
```

2. Ensure `Shuttle.toml` exists in `rust-polars/`:

```toml
name = "polars-etl-benchmark"
```

3. Deploy from `rust-polars/`:

```
cargo shuttle deploy
```

4. **Project name rules**
   If prompted to create/link a project, use **lowercase + dashes only**, 1–32 chars, no leading/trailing dash (e.g., `pandas-vs-polars-metrics`).

5. After a successful deploy, the CLI prints your URL:

```
https://<project-name>-<random>.shuttle.app
```

**Common Shuttle gotchas**

* `cargo shuttle deploy` does **not** accept `--features`.
* If you see *“Expected at least one target that Shuttle can build”*, ensure your crate exposes a **binary** with a fully-qualified `#[shuttle_runtime::main]` (the Axum router function in `main.rs`), and that it’s compiled in the environment you deploy.
* The API is **demo/representative**; don’t run the 12.7M-row CSV inside Shuttle unless you’ve explicitly provisioned for it.

---

## Warnings & Resource Notes

* The **CSV is large** (\~2 GB). Ensure disk and memory headroom.
* **Pandas** run may exceed **4–6 GB RAM**; close memory-heavy apps.
* Use **release** builds for Rust; debug builds will look unfairly slow.
* Expect numbers to vary by CPU, storage (SSD vs HDD), and thread count.

---

## Troubleshooting

* **Schema mismatch (`Int64` vs `u32`)**
  The Polars pipeline casts boolean counts to `Int64` before summing (e.g., `.cast(DataType::Int64).sum()`), preventing dtype mismatches.

* **“Data file not found”**
  Confirm the CSV path matches the code: `../data/yellow_tripdata_2015-01.csv` from the `rust-polars/` and `python-pandas/` directories.

* **Shuttle: invalid project name**
  Use **lowercase letters, digits, dashes**, no spaces, length 1–32, not starting/ending with a dash.

---

## Project Structure (Rust)

```
rust-polars/
├─ src/
│  ├─ etl.rs         # Polars Lazy ETL (load/clean/aggregate/filter/save)
│  └─ main.rs        # CLI benchmark + Axum service (Shuttle entrypoint)
├─ Cargo.toml
├─ Shuttle.toml
└─ deploy_to_shuttle.sh
```

**Key crates**

* `polars = "0.49.1"` with `lazy`, `temporal`, `strings`, `csv`
* `axum`, `tower-http` (CORS), `tokio`
* `serde`, `serde_json`, `chrono`
* `shuttle-runtime`, `shuttle-axum` for deployment

---

## Axum Template (Service Skeleton)

The service registers four GET routes and returns JSON:

```rust
#[shuttle_runtime::main]
pub async fn shuttle_main() -> shuttle_axum::ShuttleAxum {
    let router = Router::new()
        .route("/", get(health_check))
        .route("/health", get(health_check))
        .route("/benchmark", get(run_benchmark))
        .route("/info", get(get_comparison_info))
        .layer(CorsLayer::permissive());

    Ok(router.into())
}
```

> The `/benchmark` handler returns a **representative** metric payload so it’s fast/safe on small dyno resources. The CLI `main()` runs the real ETL.

---

## Contributing

* Issues and PRs welcome.
* Please open an issue with:

  * OS/arch, CPU core count
  * Rust & cargo versions; Python & Pandas versions
  * Exact command used
  * Rough system load & storage type (SSD/HDD/NVMe)

---

## License

MIT License.

---

## Acknowledgements

* NYC TLC for the open dataset
* Polars & Pandas communities
* Shuttle for Rust-native deployment

---

