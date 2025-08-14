#!/bin/bash
set -euo pipefail

echo "🚀 Deploying Polars ETL Benchmark to Shuttle"
echo "============================================"

# Run from the script's directory so paths are correct
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"


# Build with optimizations first to check for errors
echo "🔧 Building optimized release version..."
cargo build --release


echo "✅ Build successful!"
echo ""

# Deploy to Shuttle
echo "📦 Deploying to Shuttle..."
echo "Make sure you're logged in: cargo shuttle login"
echo ""

cargo shuttle deploy 

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Deployment successful!"
    echo "Your Polars ETL benchmark API is now live!"
    echo ""
    echo "Available endpoints:"
    echo "  • GET / - Health check & service info"
    echo "  • GET /benchmark - Run performance demo"
    echo "  • GET /info - Detailed comparison information"
    echo "  • GET /health - Service status"
else
    echo "❌ Deployment failed. Check the error messages above."
fi

cd ..
