#!/bin/bash
set -e

echo "=========================================="
echo "Setting up Sanctions Network Mapper"
echo "=========================================="

# Install uv (fast Python package manager)
echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Create virtual environment and install dependencies
echo "Creating virtual environment..."
uv venv .venv
source .venv/bin/activate

echo "Installing Python dependencies..."
uv pip install -e ".[dev]"

# Create data directories
echo "Creating data directories..."
mkdir -p data/raw/opensanctions
mkdir -p data/raw/corporate
mkdir -p data/processed
mkdir -p data/output
mkdir -p logs

# Set up pre-commit hooks (optional but recommended)
echo "Setting up pre-commit hooks..."
if command -v pre-commit &> /dev/null; then
    pre-commit install
fi

# Verify installation
echo ""
echo "=========================================="
echo "Verifying installation..."
echo "=========================================="
python -c "import polars; print(f'Polars: {polars.__version__}')"
python -c "import httpx; print(f'HTTPX: {httpx.__version__}')"
python -c "import rapidfuzz; print(f'RapidFuzz: {rapidfuzz.__version__}')"
python -c "import networkx; print(f'NetworkX: {networkx.__version__}')"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Quick start commands:"
echo "  1. Download sanctions data:"
echo "     python -m src.ingest.opensanctions"
echo ""
echo "  2. Run tests:"
echo "     pytest tests/ -v"
echo ""
echo "  3. Start Dagster UI:"
echo "     dagster dev"
echo ""
