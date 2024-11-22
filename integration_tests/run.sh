#!/bin/bash
set -euo pipefail

# Colors for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function for error handling
error_handler() {
    echo -e "${RED}Error occurred in script at line: $1${NC}"
    echo -e "${RED}Command that failed: ${BASH_COMMAND}${NC}"
    exit 1
}

trap 'error_handler ${LINENO}' ERR

# Load environment variables
if [ -f .env ]; then
    echo -e "${BLUE}Loading environment variables...${NC}"
    set -a
    source .env
    set +a
else
    echo -e "${RED}Error: .env file not found${NC}"
    exit 1
fi

# Install dependencies
echo -e "${BLUE}Installing dbt dependencies...${NC}"
poetry run dbt deps

# Run incremental tests
echo -e "${BLUE}Running incremental tests...${NC}"
poetry run dbt build --select tag:incremental --fail-fast

# Run full refresh tests
echo -e "${BLUE}Running full refresh tests...${NC}"
poetry run dbt build --select tag:full_refresh --full-refresh --fail-fast

# Run target ci_b tests
echo -e "${BLUE}Running target prod_etl tests...${NC}"
poetry run dbt build --select tag:ci_b --fail-fast --target ci_b

echo -e "${GREEN}All tests completed successfully!${NC}"