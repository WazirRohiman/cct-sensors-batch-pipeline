#!/bin/bash

# ═══════════════════════════════════════════════════════════════════════
# CCT Sensors Batch Pipeline - Installation Script
# ═══════════════════════════════════════════════════════════════════════
# Purpose: Automated setup and installation
# Usage: ./install.sh
# ═══════════════════════════════════════════════════════════════════════

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# ═══════════════════════════════════════════════════════════════════════
# 1. PREREQUISITE CHECKS
# ═══════════════════════════════════════════════════════════════════════

print_header "Step 1: Checking Prerequisites"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi
print_success "Docker is installed ($(docker --version))"

# Check if Docker is running
if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running"
    echo "Please start Docker and try again"
    exit 1
fi
print_success "Docker daemon is running"

# Check if Docker Compose is installed (check both plugin and standalone)
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
    COMPOSE_VERSION=$(docker compose version 2>&1 | head -n1 | grep -oP 'v[\d.]+' || echo "plugin")
    print_success "Docker Compose is installed ($COMPOSE_VERSION)"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
    print_success "Docker Compose is installed ($(docker-compose --version))"
else
    print_error "Docker Compose is not installed"
    echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

# Check disk space (need at least 5GB free)
available_space=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
if [ "$available_space" -lt 5 ]; then
    print_warning "Low disk space: ${available_space}GB available (recommend 5GB+)"
else
    print_success "Sufficient disk space: ${available_space}GB available"
fi

# ═══════════════════════════════════════════════════════════════════════
# 2. ENVIRONMENT SETUP
# ═══════════════════════════════════════════════════════════════════════

print_header "Step 2: Setting Up Environment"

# Check if .env file exists
if [ ! -f "docker/airflow/.env" ]; then
    print_warning ".env file not found"
    print_info "Creating .env from example..."

    if [ -f "docker/airflow/.env.example" ]; then
        cp docker/airflow/.env.example docker/airflow/.env
        print_success "Created .env file from example"

        # Generate secure secret key
        print_info "Generating secure secret key..."
        SECRET_KEY=$(openssl rand -hex 32)

        # Replace placeholder with actual secret key
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/<insert-your-secret-key-here>/$SECRET_KEY/" docker/airflow/.env
        else
            # Linux
            sed -i "s/<insert-your-secret-key-here>/$SECRET_KEY/" docker/airflow/.env
        fi
        print_success "Secret key generated and configured"
        print_warning "Please review docker/airflow/.env and update other credentials if needed"
    else
        print_error ".env.example not found"
        echo "Please create docker/airflow/.env manually"
        exit 1
    fi
else
    print_success ".env file exists"

    # Check if secret key is still placeholder
    if grep -q "<insert-your-secret-key-here>" docker/airflow/.env; then
        print_warning "Secret key is still placeholder"
        print_info "Generating secure secret key..."
        SECRET_KEY=$(openssl rand -hex 32)

        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s/<insert-your-secret-key-here>/$SECRET_KEY/" docker/airflow/.env
        else
            sed -i "s/<insert-your-secret-key-here>/$SECRET_KEY/" docker/airflow/.env
        fi
        print_success "Secret key generated and configured"
    fi

    # Check if AIRFLOW_IMAGE is set to base image instead of custom image
    if grep -q "^AIRFLOW_IMAGE=apache/airflow:" docker/airflow/.env; then
        print_warning "AIRFLOW_IMAGE is set to base image, updating to custom image name..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s|^AIRFLOW_IMAGE=apache/airflow:.*|AIRFLOW_IMAGE=cct-airflow:latest|" docker/airflow/.env
        else
            sed -i "s|^AIRFLOW_IMAGE=apache/airflow:.*|AIRFLOW_IMAGE=cct-airflow:latest|" docker/airflow/.env
        fi
        print_success "AIRFLOW_IMAGE updated to cct-airflow:latest"
    fi
fi

# Create data directories if they don't exist
print_info "Creating data directories..."
mkdir -p data/raw data/staged data/normalised data/duckdb data/quarantine data/logs
print_success "Data directories created"

# Set correct permissions for Airflow
print_info "Setting directory permissions..."
# Airflow runs as UID 50000, set appropriate permissions
if [ "$(uname)" = "Linux" ]; then
    if sudo -n true 2>/dev/null; then
        # Can run sudo without password
        sudo chown -R 50000:100 data/ 2>/dev/null && \
        sudo chmod -R 775 data/ 2>/dev/null && \
        print_success "Permissions configured (ownership: 50000:100, mode: 775)" || {
            print_warning "Could not set all permissions"
            print_info "Airflow init container will handle permissions"
        }
    else
        # Need to prompt for sudo
        if sudo chown -R 50000:100 data/ 2>/dev/null && \
           sudo chmod -R 775 data/ 2>/dev/null; then
            print_success "Permissions configured (ownership: 50000:100, mode: 775)"
        else
            print_warning "Could not set all permissions"
            print_info "Airflow init container will handle permissions"
        fi
    fi
else
    # Non-Linux systems (macOS, etc.)
    chmod -R 775 data/ 2>/dev/null || {
        print_warning "Could not set permissions"
        print_info "Airflow init container will handle permissions"
    }
    print_success "Permissions configured"
fi

# ═══════════════════════════════════════════════════════════════════════
# 3. BUILD AND START SERVICES
# ═══════════════════════════════════════════════════════════════════════

print_header "Step 3: Building and Starting Services"

cd docker/airflow

print_info "Building Docker images (this may take 3-5 minutes on first run)..."
if ! $DOCKER_COMPOSE build; then
    print_error "Docker build failed"
    print_info "Trying with --no-cache flag..."
    if ! $DOCKER_COMPOSE build --no-cache; then
        print_error "Docker build failed even with --no-cache"
        print_info "Check the error messages above"
        exit 1
    fi
fi

print_success "Docker images built"

print_info "Starting services..."
$DOCKER_COMPOSE up -d

print_success "Services started"

# ═══════════════════════════════════════════════════════════════════════
# 4. WAIT FOR SERVICES TO BE READY
# ═══════════════════════════════════════════════════════════════════════

print_header "Step 4: Waiting for Services to Initialize"

print_info "This may take 2-5 minutes on first run..."
print_info "Waiting for Airflow webserver to be ready..."

# Wait for webserver to be healthy (max 5 minutes)
max_attempts=60
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if $DOCKER_COMPOSE ps | grep airflow-webserver | grep -q "healthy"; then
        print_success "Airflow webserver is ready"
        break
    fi

    if [ $attempt -eq 0 ]; then
        echo -n "Progress: "
    fi
    echo -n "."
    sleep 5
    attempt=$((attempt + 1))

    if [ $attempt -eq $max_attempts ]; then
        echo ""
        print_error "Timeout waiting for services to start"
        print_info "Check logs with: $DOCKER_COMPOSE logs"
        exit 1
    fi
done

if [ $attempt -gt 0 ]; then
    echo ""  # New line after progress dots
fi

# ═══════════════════════════════════════════════════════════════════════
# 5. VERIFY INSTALLATION
# ═══════════════════════════════════════════════════════════════════════

print_header "Step 5: Verifying Installation"

# Check if all services are running
services=("postgres" "airflow-webserver" "airflow-scheduler" "airflow-triggerer" "viewer")
all_running=true

for service in "${services[@]}"; do
    if $DOCKER_COMPOSE ps | grep "$service" | grep -q "Up"; then
        print_success "$service is running"
    else
        print_error "$service is not running"
        all_running=false
    fi
done

if [ "$all_running" = false ]; then
    print_error "Some services failed to start"
    print_info "Check logs with: $DOCKER_COMPOSE logs [service-name]"
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════════
# 6. SUCCESS MESSAGE
# ═══════════════════════════════════════════════════════════════════════

print_header "Installation Complete! 🎉"

echo -e "${GREEN}All services are up and running!${NC}\n"

echo "Access Points:"
echo -e "  ${BLUE}Airflow UI:${NC}  http://localhost:8080"
echo -e "    Credentials: ${YELLOW}admin${NC} / ${YELLOW}admin${NC}"
echo ""
echo -e "  ${BLUE}Jupyter:${NC}     http://localhost:8888"
echo -e "    (No token required)"
echo ""

echo "Next Steps:"
echo "  1. Open Airflow UI: http://localhost:8080"
echo "  2. Unpause the 'master_pipeline' DAG"
echo "  3. Trigger a manual run (play button)"
echo "  4. Monitor progress in the UI"
echo ""

echo "Common Commands:"
echo -e "  ${BLUE}View logs:${NC}       cd docker/airflow && $DOCKER_COMPOSE logs -f"
echo -e "  ${BLUE}Stop services:${NC}   cd docker/airflow && $DOCKER_COMPOSE down"
echo -e "  ${BLUE}Restart:${NC}         cd docker/airflow && $DOCKER_COMPOSE restart"
echo ""

echo "Documentation:"
echo "  • docs/PROJECT_STARTUP.md - Quick start guide"
echo "  • docs/PIPELINE_VISUAL_GUIDE.md - Pipeline flow"
echo "  • docs/MONITORING_OBSERVABILITY.md - Monitoring guide"
echo "  • docs/DOCKER_SERVICES_GUIDE.md - Container architecture"
echo ""

print_success "Setup completed successfully!"
