#!/bin/bash

echo "🔥 PANDAS VS POLARS: THE ULTIMATE SHOWDOWN"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Config (override via env if you want)
COOL_DOWN=${COOL_DOWN:-120}     # try 60 or 120
POLARS_THREADS=${POLARS_THREADS:-$(nproc)}
PYTHON_BIN=${PYTHON_BIN:-python}  # use same interpreter for both steps

# Apply thread caps
export POLARS_MAX_THREADS="$POLARS_THREADS"
export RAYON_NUM_THREADS="$POLARS_THREADS"
echo "🧵 Using $POLARS_THREADS threads for Polars/Rayon with $PYTHON_BIN for Python"
echo ""

# Create results directory
mkdir -p results
echo "📁 Created results directory"

# Check if dataset exists
if [ ! -f "./data/yellow_tripdata_2015-01.csv" ]; then
    echo -e "${RED}❌ Dataset not found!${NC}"
    echo "Please download the NYC Yellow Taxi dataset (January 2015) and place it in ./data/"
    echo "Dataset URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    exit 1
fi

echo -e "${GREEN}✅ Dataset found!${NC}"
echo ""

# Run Python Pandas benchmark
echo -e "${BLUE}🐼 RUNNING PYTHON PANDAS BENCHMARK${NC}"
echo "=================================="
pushd python-pandas >/dev/null
$PYTHON_BIN pandas_etl.py
popd >/dev/null
echo ""

# Cool down to reduce thermal throttling
if [ "$COOL_DOWN" -gt 0 ]; then
  echo "🧊 Cooling down for ${COOL_DOWN}s before running Polars..."
  sleep "$COOL_DOWN"
  echo ""
fi

# Build Polars once (if needed), then run from inside rust-polars so relative paths match
echo -e "${YELLOW}🚀 RUNNING RUST POLARS BENCHMARK${NC}"
echo "================================="
if [ ! -x "./rust-polars/target/release/polars-etl-benchmark" ]; then
  echo "🛠️  Building Polars release binary (first time only)..."
  (cd rust-polars && cargo build --release) || { echo -e "${RED}Build failed${NC}"; exit 1; }
else
  echo "✅ Using existing Polars release binary"
fi

# Run in rust-polars so the app's ../data/... path resolves correctly
(
  cd rust-polars
  ./target/release/polars-etl-benchmark
)
echo ""

# Generate comparison report (use the same Python interpreter)
echo -e "${GREEN}📊 GENERATING PERFORMANCE COMPARISON${NC}"
echo "===================================="

$PYTHON_BIN << 'PYTHON_EOF'
import json
import pandas as pd
import os

def load_metrics(filename):
   try:
       with open(f'results/{filename}', 'r') as f:
           return json.load(f)
   except FileNotFoundError:
       print(f"⚠️  Could not find {filename}")
       return {}

# Load metrics from both benchmarks
pandas_metrics = load_metrics('pandas_metrics.json')
polars_metrics = load_metrics('polars_metrics.json')

if not pandas_metrics or not polars_metrics:
   print("❌ Could not load benchmark results. Please run benchmarks first.")
   exit(1)

print("\n" + "="*60)
print("🏆 PERFORMANCE COMPARISON RESULTS")
print("="*60)

# Create detailed comparison
operations = [
   ('load_time', 'Data Loading'),
   ('clean_time', 'Data Cleaning'),
   ('aggregate_time', 'Aggregations'),
   ('sort_filter_time', 'Sort & Filter'),
   ('save_time', 'Save Results'),
   ('total_time', 'TOTAL TIME')
]

comparison_data = []
total_pandas_time = 0
total_polars_time = 0

print(f"{'Operation':<20} {'Pandas':<12} {'Polars':<12} {'Speedup':<10} {'Winner'}")
print("-" * 65)

for key, name in operations:
   if key in pandas_metrics and key in polars_metrics:
       pandas_time = float(pandas_metrics[key])
       polars_time = float(polars_metrics[key])
       
       if key == 'total_time':
           total_pandas_time = pandas_time
           total_polars_time = polars_time
       
       speedup = pandas_time / polars_time if polars_time > 0 else 0
       winner = "🚀 Polars" if polars_time < pandas_time else "🐼 Pandas"
       
       print(f"{name:<20} {pandas_time:<12.2f} {polars_time:<12.2f} {speedup:<10.1f}x {winner}")
       
       comparison_data.append({
           'Operation': name,
           'Pandas_Time_s': f"{pandas_time:.2f}",
           'Polars_Time_s': f"{polars_time:.2f}",
           'Speedup': f"{speedup:.1f}x",
           'Time_Saved_s': f"{pandas_time - polars_time:.2f}"
       })

print("-" * 65)

# Overall performance summary
if total_polars_time > 0:
   overall_speedup = total_pandas_time / total_polars_time
   time_saved = total_pandas_time - total_polars_time
   efficiency_gain = ((time_saved / total_pandas_time) * 100)
   
   print(f"\n🎯 OVERALL PERFORMANCE SUMMARY:")
   print(f"   • Polars is {overall_speedup:.1f}x faster than Pandas")
   print(f"   • Time saved: {time_saved:.2f} seconds ({efficiency_gain:.1f}% faster)")
   print(f"   • On a dataset of {pandas_metrics.get('rows_loaded', 'N/A'):,} rows")

# Data processing insights
print(f"\n📈 DATA PROCESSING INSIGHTS:")
if 'rows_loaded' in pandas_metrics:
   rows = int(pandas_metrics['rows_loaded'])
   pandas_throughput = rows / total_pandas_time if total_pandas_time > 0 else 0
   polars_throughput = rows / total_polars_time if total_polars_time > 0 else 0
   
   print(f"   • Pandas throughput: {pandas_throughput:,.0f} rows/second")
   print(f"   • Polars throughput: {polars_throughput:,.0f} rows/second")
   if pandas_throughput > 0:
       print(f"   • Throughput improvement: {(polars_throughput/pandas_throughput):.1f}x")

# Memory comparison (if available)
if 'peak_memory' in pandas_metrics:
   pandas_memory = pandas_metrics['peak_memory']
   print(f"   • Pandas peak memory: {pandas_memory:.1f} MB")
   if 'peak_memory_mb' in polars_metrics:
       print(f"   • Polars peak memory: {polars_metrics['peak_memory_mb']:.1f} MB")

# Save detailed comparison
import pandas as pd
df = pd.DataFrame(comparison_data)
df.to_csv('results/performance_comparison.csv', index=False)
print(f"\n💾 Detailed results saved to:")
print(f"   • results/performance_comparison.csv")
print(f"   • results/pandas_metrics.json") 
print(f"   • results/polars_metrics.json")

print("\n" + "="*60)
print("🎉 BENCHMARK COMPLETE!")
print("="*60)
print("🚀 Try the live demo: Deploy this Polars app with Shuttle!")
print("   cd rust-polars && cargo shuttle deploy")
print("="*60)
PYTHON_EOF

echo ""
echo -e "${GREEN}✨ Benchmark comparison complete!${NC}"
echo "Check the results/ directory for detailed output files."
