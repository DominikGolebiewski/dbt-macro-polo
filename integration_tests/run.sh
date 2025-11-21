source .env
# Install dependencies if not present
dbt deps --profiles-dir .

# Load seeds
dbt seed --profiles-dir .

# 1. UNIT TESTS (Logic verification)
echo "Running Unit Tests..."
dbt run --select unit_tests --profiles-dir .
dbt test --select unit_tests --profiles-dir .

# 2. INTEGRATION TESTS (Database execution verification)
echo "Running Integration Tests..."
dbt run --select integration --profiles-dir .
dbt test --select integration --profiles-dir .
