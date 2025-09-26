#!/bin/bash
# Fix DuckDB permissions for cross-container access
# This script ensures Jupyter container can access DuckDB files created by Airflow

echo "🔧 Fixing DuckDB permissions for cross-container access..."

# Check if running in Docker context
if [ -f "/.dockerenv" ]; then
    echo "   Running inside container"
    DUCKDB_PATH="/home/jovyan/work/data/duckdb/"
else
    echo "   Running on host system"
    DUCKDB_PATH="data/duckdb/"
fi

# Ensure directory exists
mkdir -p "$DUCKDB_PATH"

# Set proper ownership and permissions
# jovyan (UID 1000) in Jupyter container needs read/write access
chown -R 1000:100 "$DUCKDB_PATH" 2>/dev/null || {
    echo "⚠️  Cannot change ownership (need root privileges)"
    echo "   Attempting to fix permissions only..."
    chmod -R 775 "$DUCKDB_PATH" 2>/dev/null || {
        echo "❌ Cannot fix permissions - run as root or with sudo"
        exit 1
    }
}

# Set directory permissions for future files
chmod -R 775 "$DUCKDB_PATH"

echo "✅ DuckDB permissions fixed successfully!"
echo "   - Jupyter container (jovyan) can now read/write DuckDB files"
echo "   - Airflow container can continue to create/update files"

ls -la "$DUCKDB_PATH" 2>/dev/null | head -5