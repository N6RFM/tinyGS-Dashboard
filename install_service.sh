#!/bin/bash
# Install TinyGS Dashboard as a systemd service (auto-starts on boot,
# auto-restarts if it crashes). Run this with sudo: sudo bash install_service.sh
# Code written by N6RFM with help from Claude AI
#
# IMPORTANT GOTCHA: Restart=always below means systemd will immediately
# relaunch the server if you `kill` it manually - if you're trying to run a
# one-off manual instance for testing (e.g. `python3 tinygs_server.py`
# directly) while this service is also enabled, you'll get two processes
# fighting over the same serial port ("device disconnected or multiple
# access" errors). Before running a manual instance, stop the service first:
#   sudo systemctl stop tinygs-dashboard
# and remember it'll come back on next boot unless you also:
#   sudo systemctl disable tinygs-dashboard

set -e

# This script must be run with sudo, which means $HOME and `whoami` reflect
# ROOT, not the person who typed the sudo command - using either directly
# previously generated a service that ran as User=root out of /root/..., a
# path that doesn't exist, causing a 203/EXEC failure at every start.
# $SUDO_USER is set BY sudo itself to the original invoking user, so use that
# instead (falling back to whoami only for the unusual case of actually being
# logged in as root directly, not via sudo).
USER_NAME="${SUDO_USER:-$(whoami)}"
USER_HOME=$(getent passwd "$USER_NAME" | cut -d: -f6)
if [ -z "$USER_HOME" ]; then
    echo "❌ Could not determine home directory for user '$USER_NAME'."
    exit 1
fi

# Must match install.sh's PROJECT_DIR exactly - both are fixed at
# <home>/tinyGS-Dashboard (matching the GitHub repo's name/casing) regardless
# of where you run either script from.
PROJECT_DIR="$USER_HOME/tinyGS-Dashboard"

# Sanity check before writing a service file that points nowhere useful.
if [ ! -f "$PROJECT_DIR/venv/bin/python" ]; then
    echo "❌ $PROJECT_DIR/venv/bin/python not found."
    echo "   Run install.sh as $USER_NAME first (not with sudo) to create the venv, then re-run this script."
    exit 1
fi

echo "📌 Installing service for user: $USER_NAME (home: $USER_HOME)"
echo "📌 Project directory: $PROJECT_DIR"

sudo tee /etc/systemd/system/tinygs-dashboard.service << EOF
[Unit]
Description=TinyGS Serial Dashboard
After=network.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/venv/bin:/usr/bin
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/tinygs_server.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable tinygs-dashboard
sudo systemctl start tinygs-dashboard

echo "✅ Service installed and started!"
echo "   Status: sudo systemctl status tinygs-dashboard"
echo "   Logs:   sudo journalctl -u tinygs-dashboard -f"
echo "   URL:    http://localhost:5000"
