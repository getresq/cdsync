#!/bin/bash
# Install pre-push hook via pre-commit.
set -euo pipefail

if [ ! -d ".git" ] && [ ! -f ".git" ]; then
    echo "Error: run from the repo root" >&2
    exit 1
fi

uvx pre-commit install --hook-type pre-push
