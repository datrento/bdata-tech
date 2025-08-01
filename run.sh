#!/bin/bash
# filepath: /home/abiget/Documents/bdata/run.sh

# Color setup
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Price Intelligence Setup${NC}"

# Function for errors
error() {
  echo -e "${RED}ERROR: $1${NC}" >&2
  exit 1
}

# Check for Docker
command -v docker >/dev/null 2>&1 || error "Docker is not installed"

# Use the appropriate docker compose command
if command -v docker-compose >/dev/null 2>&1; then
  compose="docker-compose"
else
  compose="docker compose"
fi

# Ensure .env exists
if [ ! -f .env ]; then
  echo "Creating .env from example..."
  cp .env.example .env || error "Could not create .env file"
fi

# Ensure requirements.txt is in dashboard directory
# if [ ! -f dashboard/requirements.txt ] && [ -f requirements.txt ]; then
#   echo "Copying requirements.txt to dashboard directory..."
#   cp requirements.txt dashboard/ || error "Could not copy requirements.txt"
# fi

# Parse command
case "$1" in
  start|up)
    echo "Starting services..."
    $compose up -d --remove-orphans
    echo -e "${GREEN}Services started!${NC}"
    # echo "Dashboard: http://$(grep STREAMLIT_HOST .env | cut -d= -f2):$(grep STREAMLIT_PORT .env | cut -d= -f2)"
    echo "Kafka UI: http://$(grep KAFKA_UI_HOST .env | cut -d= -f2):$(grep KAFKA_UI_PORT .env | cut -d= -f2)"
    echo "Adminer UI: http://$(grep ADMINER_HOST .env | cut -d= -f2):$(grep ADMINER_PORT .env | cut -d= -f2)"
    ;;
  stop|down)
    echo "Stopping services..."
    $compose down -v
    ;;
  restart)
    echo "Restarting services..."
    $compose restart
    ;;
  logs)
    echo "Showing logs..."
    $compose logs -f
    ;;
  build)
    echo "Building services..."
    $compose build --no-cache
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|logs|build}"
    echo "  start   - Start all services"
    echo "  stop    - Stop all services"
    echo "  restart - Restart all services"
    echo "  logs    - Show logs"
    echo "  build   - Rebuild services"
    ;;
esac