#!/bin/bash
set -e

echo "=========================================="
echo "Setting up Sanctions Network Mapper"
echo "=========================================="

cd /workspaces/sanctions-network-mapper 2>/dev/null || cd "$(dirname "$0")/.."

# Install uv (fast Python package manager)
echo ""
echo "Installing uv package manager..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
uv venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
echo ""
echo "Installing Python dependencies..."
uv pip install -e .

# Create data directories
echo ""
echo "Creating data directories..."
mkdir -p data/raw/opensanctions
mkdir -p data/raw/corporate
mkdir -p data/processed
mkdir -p data/output
mkdir -p logs

# Add activation to bashrc for new terminals
echo ""
echo "Configuring shell..."
echo 'source /workspaces/sanctions-network-mapper/.venv/bin/activate 2>/dev/null || true' >> ~/.bashrc

# Verify installation
echo ""
echo "=========================================="
echo "Verifying installation..."
echo "=========================================="
python -c "import polars; print(f'  Polars: {polars.__version__}')"
python -c "import httpx; print(f'  HTTPX: {httpx.__version__}')"
python -c "import rapidfuzz; print(f'  RapidFuzz: {rapidfuzz.__version__}')"
python -c "import networkx; print(f'  NetworkX: {networkx.__version__}')"
python -c "import pydantic; print(f'  Pydantic: {pydantic.__version__}')"
python -c "import rich; print(f'  Rich: {rich.__version__}')"
python -c "import typer; print(f'  Typer: {typer.__version__}')"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Quick start commands:"
echo ""
echo "  1. Verify setup:"
echo "     python quickstart.py"
echo ""
echo "  2. Download sanctions data (~500MB):"
echo "     snm ingest opensanctions"
echo ""
echo "  3. Parse the data:"
echo "     python -m src.ingest.opensanctions"
echo ""
echo "  4. View statistics:"
echo "     snm analyze stats"
echo ""
