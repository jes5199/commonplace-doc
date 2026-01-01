#!/bin/bash
source ~/.bashrc 2>/dev/null || true
cd /home/jes/commonplace/workspace
exec ~/.local/bin/uv run --project /home/jes/bartleby python /home/jes/bartleby/bartleby.py
