#!/bin/bash

echo " Starting Ecommerce Data Pipeline setup..."

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | xargs)
else
  echo " .env file not found. Please create it from .env.example"
  exit 1
fi

# Create Python virtual environment
echo " Creating virtual environment..."
python -m venv venv
source venv/Scripts/activate || source venv/bin/activate

# Install Python dependencies
echo " Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# PostgreSQL database & schema setup
echo " Setting up PostgreSQL schemas..."

psql -h $DB_HOST -U $DB_USER -d $DB_NAME <<EOF
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS warehouse;
EOF

echo " Setup completed successfully!"
