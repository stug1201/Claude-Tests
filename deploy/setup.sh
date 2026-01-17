#!/bin/bash
# =============================================================================
# TWAP Alert Service - AWS EC2 Setup Script
#
# Designed for: AWS Free Tier EC2 t2.micro (Amazon Linux 2023 or Ubuntu 22.04)
#
# Usage:
#   1. Launch EC2 t2.micro instance
#   2. SSH into instance
#   3. Clone repo: git clone https://github.com/YOUR_USER/Claude-Tests.git
#   4. Run: cd Claude-Tests && chmod +x deploy/setup.sh && ./deploy/setup.sh
#   5. Edit config: nano /opt/twap-alerts/config.json
#   6. Start service: sudo systemctl start twap-alerts
# =============================================================================

set -e

echo "========================================"
echo "TWAP Alert Service Setup"
echo "========================================"

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    echo "Cannot detect OS"
    exit 1
fi

echo "Detected OS: $OS"

# Install dependencies based on OS
echo ""
echo "Installing system dependencies..."

if [ "$OS" == "amzn" ] || [ "$OS" == "amazon" ]; then
    # Amazon Linux
    sudo yum update -y
    sudo yum install -y python3 python3-pip git
elif [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
    # Ubuntu/Debian
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip python3-venv git
else
    echo "Unsupported OS: $OS"
    echo "Please install Python 3.9+ manually"
    exit 1
fi

# Create application directory
echo ""
echo "Setting up application directory..."

APP_DIR="/opt/twap-alerts"
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# Copy files
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cp -r $REPO_DIR/execution/*.py $APP_DIR/
cp $REPO_DIR/requirements.txt $APP_DIR/

# Create virtual environment
echo ""
echo "Creating Python virtual environment..."

cd $APP_DIR
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."

pip install --upgrade pip
pip install -r requirements.txt

# Create config from template if it doesn't exist
if [ ! -f "$APP_DIR/config.json" ]; then
    echo ""
    echo "Creating config template..."
    cat > $APP_DIR/config.json << 'EOF'
{
  "telegram_bot_token": "YOUR_BOT_TOKEN",
  "telegram_channel_id": "YOUR_CHANNEL_ID",
  "telegram_admin_id": 0,
  "tickers": [
    {"symbol": "BTCUSDT", "exchange": "binance", "market_type": "perpetual", "enabled": true},
    {"symbol": "ETHUSDT", "exchange": "binance", "market_type": "perpetual", "enabled": true}
  ],
  "analysis_interval_sec": 30,
  "min_buffer_sec": 120,
  "buffer_minutes": 30,
  "min_confidence": "LOW",
  "alert_on_updates": false
}
EOF
fi

# Install systemd service
echo ""
echo "Installing systemd service..."

sudo cat > /tmp/twap-alerts.service << EOF
[Unit]
Description=TWAP Alert Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/twap_telegram_alerts.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

# Logging
StandardOutput=append:/var/log/twap-alerts.log
StandardError=append:/var/log/twap-alerts.log

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/twap-alerts.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable twap-alerts

# Create log file
sudo touch /var/log/twap-alerts.log
sudo chown $USER:$USER /var/log/twap-alerts.log

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Edit config with your Telegram credentials:"
echo "   nano $APP_DIR/config.json"
echo ""
echo "2. Get your Telegram credentials:"
echo "   - Bot token: Message @BotFather, create bot, copy token"
echo "   - Channel ID: Create channel, add bot as admin, send message,"
echo "                 visit https://api.telegram.org/bot<TOKEN>/getUpdates"
echo "   - Admin ID: Message @userinfobot to get your user ID"
echo ""
echo "3. Start the service:"
echo "   sudo systemctl start twap-alerts"
echo ""
echo "4. Check status:"
echo "   sudo systemctl status twap-alerts"
echo ""
echo "5. View logs:"
echo "   tail -f /var/log/twap-alerts.log"
echo ""
echo "Service commands:"
echo "   sudo systemctl start twap-alerts   # Start"
echo "   sudo systemctl stop twap-alerts    # Stop"
echo "   sudo systemctl restart twap-alerts # Restart"
echo "   sudo systemctl status twap-alerts  # Status"
echo ""
