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

cd integration_tests || {
    echo -e "${RED}Failed to change to integration_tests directory${NC}"
    exit 1
}

# Install dependencies
echo -e "${BLUE}Installing dbt dependencies...${NC}"
poetry run dbt deps

# Run incremental tests
echo -e "${BLUE}Running incremental tests...${NC}"
poetry run dbt build --select tag:incremental_test --fail-fast --exclude tag:target_test

# Run target tests
echo -e "${BLUE}Running target tests...${NC}"
poetry run dbt build --select tag:target_test --fail-fast --exclude tag:incremental_test

# Run full refresh tests
echo -e "${BLUE}Running full refresh tests...${NC}"
poetry run dbt build --select tag:full_refresh_test --full-refresh --fail-fast

echo -e "${GREEN}All tests completed successfully!${NC}"