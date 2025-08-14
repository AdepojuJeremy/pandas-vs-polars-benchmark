#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/download_data.sh              # defaults to CSV (BIG ~1.9GB)
#   ./scripts/download_data.sh csv         # explicit CSV
#   ./scripts/download_data.sh parquet     # smaller Parquet (~200MB)

FORMAT="${1:-csv}"
mkdir -p data

case "$FORMAT" in
  csv)
    # Very large file (~1.9 GB)
    URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv"
    OUT="data/yellow_tripdata_2015-01.csv"
    ;;
  parquet)
    # Much smaller; if you choose this, adjust your code to read Parquet instead of CSV
    URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet"
    OUT="data/yellow_tripdata_2015-01.parquet"
    ;;
  *)
    echo "Unknown format: $FORMAT (use 'csv' or 'parquet')" >&2
    exit 1
    ;;
esac

echo "Downloading $URL -> $OUT"
curl -L --fail --progress-bar "$URL" -o "$OUT"
echo "Done. File details:"
ls -lh "$OUT"
