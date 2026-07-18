#!/bin/bash
# Install TinyGS Dashboard as a systemd service (auto-starts on boot,
# auto-restarts if it crashes).
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

USER_NAME=$(whoami)
# Must match install.sh's PROJECT_DIR exactly - both are fixed at
# $HOME/tinygs-dashboard regardless of where you run either script from.
PROJECT_DIR="$HOME/tinygs-dashboard"

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
