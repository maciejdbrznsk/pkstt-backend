#!/usr/bin/env bash
set -euo pipefail

# VM hardening & Docker readiness helper for Debian 13 (172.16.8.108)
# Run as user `pkstt` with sudo privileges.

echo "[1/4] Checking Docker & Compose..."
if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker Engine & Compose plugin..."
    sudo apt-get update
    sudo apt-get install -y ca-certificates curl gnupg ufw fail2ban
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo usermod -aG docker "$USER"
    echo "Added $USER to docker group (re-login required for group change to take effect)."
else
    echo "Docker already installed: $(docker --version)"
fi

echo "[2/4] Configuring UFW firewall (SSH only; API runs through Cloudflare Tunnel)..."
sudo apt-get install -y ufw fail2ban
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw --force enable
sudo systemctl enable --now fail2ban

echo "[3/4] Checking host PostgreSQL service..."
if systemctl is-active --quiet postgresql 2>/dev/null; then
    echo "NOTE: Host postgresql service is currently running."
    echo "Since PostgreSQL runs inside Docker Compose, you can stop & disable the unused host service:"
    echo "  sudo systemctl disable --now postgresql"
fi

echo "[4/4] Done. Next steps:"
echo "  1. Copy deploy/.env.example -> .env and set POSTGRES_PASSWORD + CLOUDFLARE_TUNNEL_TOKEN"
echo "  2. Run: docker compose --profile tunnel up -d --build"
