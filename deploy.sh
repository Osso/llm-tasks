#!/bin/bash
set -euo pipefail

cargo install --force --path . --root ~/.local
echo "Installed lt to ~/.local/bin/"
