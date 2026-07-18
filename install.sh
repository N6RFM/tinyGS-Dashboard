#!/bin/bash
# TinyGS Dashboard - Linux Mint Install Script
# Code written by N6RFM with help from Claude AI
# Run this after extracting the zip
#
# IMPORTANT: this always installs to $HOME/tinygs-dashboard, REGARDLESS of
# what folder you extracted the zip into or ran this script from. If you
# extracted to e.g. ~/TinyGS_Serial_Dashboard and later can't find the venv
# or can't run the server from that folder, that's why - everything actually
# lives in ~/tinygs-dashboard. Always cd there to run the server:
#   cd ~/tinygs-dashboard && source venv/bin/activate && python3 tinygs_server.py

set -e

echo "=========================================="
echo "🛰️  TinyGS Dashboard Installer"
echo "=========================================="

# Check if running on Linux Mint (or compatible)
if ! command -v apt &> /dev/null; then
    echo "⚠️  This installer is for Debian/Ubuntu-based systems (Linux Mint)"
    exit 1
fi

# Install system dependencies
echo "📦 Installing system packages..."
sudo apt update
sudo apt install -y python3-pip python3-venv python3-serial

# Create project directory. NOTE: this is intentionally fixed at
# $HOME/tinygs-dashboard rather than "wherever this script happens to be run
# from" - see the header comment above for why that matters.
PROJECT_DIR="$HOME/tinygs-dashboard"
echo "📌 Installing to: $PROJECT_DIR (this is fixed, regardless of the current folder name)"
mkdir -p "$PROJECT_DIR"

# Copy files to home directory if not already there
if [ "$(pwd)" != "$PROJECT_DIR" ]; then
    echo "📁 Copying files to $PROJECT_DIR..."
    cp tinygs_server.py requirements.txt "$PROJECT_DIR/"
fi

cd "$PROJECT_DIR"

# Create virtual environment
echo "🐍 Creating Python virtual environment..."
rm -rf venv  # clear out any partial venv left over from a previous failed run

# python3 -m venv can silently fail to produce a working venv if the
# installed python3-venv package doesn't match the running Python's minor
# version (a real issue seen on Ubuntu/Mint where multiple Python versions
# coexist). If the first attempt fails, retry once with the exact
# version-specific package (e.g. python3.12-venv) before giving up for real.
if ! python3 -m venv venv; then
    echo "⚠️  'python3 -m venv' failed - retrying with the version-specific venv package..."
    PYVER=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
    sudo apt install -y "python${PYVER}-venv" || true
    python3 -m venv venv
fi

# Verify the venv was actually created before trusting it - don't just
# assume success and silently continue with a broken/missing environment.
if [ ! -f "venv/bin/activate" ]; then
    echo "❌ Failed to create the virtual environment (venv/bin/activate is missing)."
    echo "   Try running manually: python3 -m venv venv"
    echo "   and check the error message it prints."
    exit 1
fi

source venv/bin/activate

# Install Python dependencies
echo "📥 Installing Python packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Create logs directory
mkdir -p logs

# Add user to dialout group for serial port access
echo "🔌 Adding user to dialout group (serial port access)..."
sudo usermod -a -G dialout "$USER"

echo ""
echo "=========================================="
echo "✅ Installation complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. LOG OUT and log back in (for dialout group)"
echo "  2. Plug in your ESP32"
echo "  3. Run: cd ~/tinygs-dashboard && source venv/bin/activate && python3 tinygs_server.py"
echo "  4. Open http://localhost:5000 in your browser"
echo ""
echo "If the port won't connect or keeps disconnecting, see the Troubleshooting"
echo "section in README.md - ModemManager and brltty are common culprits on"
echo "Mint/Ubuntu, since both auto-probe newly-plugged USB-serial devices."
echo ""
echo "To auto-start on boot, run:"
echo "  sudo bash install_service.sh"
echo ""
