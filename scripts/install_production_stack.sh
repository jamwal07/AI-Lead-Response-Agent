#!/bin/bash
# 🚀 Production Stack Installer (Option B)
# Installs PostgreSQL, Redis, PM2, and secures the server.
# Usage: sudo ./scripts/install_production_stack.sh

set -e # Exit on error

echo "📦 Updating Package List..."
apt-get update

# 1. Install Dependencies
echo "🔧 Installing System Dependencies..."
apt-get install -y python3-pip python3-venv postgresql postgresql-contrib redis-server nginx ufw build-essential libpq-dev

# 2. Secure Database (Firewall)
echo "🛡️ Configuring Firewall..."
ufw allow OpenSSH
ufw allow 'Nginx Full'
# Explicitly DENY external access to Postgres (5432) and Redis (6379)
ufw deny 5432
ufw deny 6379
ufw --force enable
echo "✅ Firewall Active. DB ports are locked."

# 3. Configure Redis
echo "⚡ Configuring Redis..."
# Ensure Redis is supervised by systemd
sed -i 's/supervised no/supervised systemd/' /etc/redis/redis.conf
systemctl restart redis.service
systemctl enable redis.service

# 4. Configure PostgreSQL
echo "🐘 Configuring PostgreSQL..."
systemctl start postgresql
systemctl enable postgresql

# Create Database and User (Interactive-less)
# We set a default password 'plumber_strong_password' - CHANGE THIS LATER in .env
sudo -u postgres psql -c "CREATE DATABASE plumber_db;" || echo "DB exists"
sudo -u postgres psql -c "CREATE USER plumber_user WITH PASSWORD 'plumber_strong_password';" || echo "User exists"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET client_encoding TO 'utf8';"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET default_transaction_isolation TO 'read committed';"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET timezone TO 'UTC';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE plumber_db TO plumber_user;"

# 5. Install PM2 (Process Manager) & Log Maintenance
echo "🚀 Installing PM2..."
if ! command -v npm &> /dev/null
then
    apt-get install -y nodejs npm
fi
npm install pm2 -g
# RISK FIX: Install Log Rotation to prevent disk overflow
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 10M
pm2 set pm2-logrotate:retain 7
pm2 startup systemd

echo "✅ Production Stack Installed!"
echo "------------------------------------------------"
echo "Next Steps:"
echo "1. Update your .env file:"
echo "   DATABASE_URL=postgresql://plumber_user:plumber_strong_password@localhost/plumber_db"
echo "   REDIS_URL=redis://localhost:6379/0"
echo "2. Run the migration script: python3 scripts/migrate_to_postgres.py"
echo "3. Start services: ./scripts/run_pm2.sh"
echo "------------------------------------------------"
