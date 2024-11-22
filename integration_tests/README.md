### Run the Integration Tests

To run the integration tests on your local machine, you can use the provided shell script or execute the steps manually. Below are the instructions for both methods:

#### Using the Shell Script

- Open the command line terminal.
- Navigate to the root folder of the package.
- Execute the shell script by running:
  ```shell
  ./run.sh
  ```

#### Running Manually

If you prefer to run the integration tests manually, follow these steps in the shell:

1. **Set Paths to Data Files:**
   ```shell
   filepath_incremental="data/materialisation/data_atheon_incremental.csv"
   filepath_restated="data/materialisation/data_atheon_insert_by_replace_restated.csv"
   filepath_original="data/materialisation/data_atheon_insert_by_replace.csv"
   ```

2. **Prepare the Record for Incremental Processing:**
   ```shell
   record_incremental="\n2022-10-08,#retailer_a,#product_1,1.66,1.1,6,#2022-10-08#retailer_a#product_1,2022-10-08 10:55:17"
   ```

3. **Navigate to the Integration Tests Directory:**
   ```shell
   cd integration_tests || exit
   ```

4. **Install dbt Dependencies:**
   ```shell
   dbt deps || exit 1
   ```

5. **Refresh Seeds and Run Models:**
   ```shell
   dbt seed --full-refresh || exit 1
   dbt run --full-refresh || exit 1
   ```

6. **Insert a New Record for Incremental Load:**
   ```shell
   echo -en "$record_incremental" >> "$filepath_incremental"
   ```

7. **Replace Original File with Restated Data:**
   ```shell
   cp "$filepath_restated" "$filepath_original"
   ```

8. **Refresh Seeds with New Records:**
   ```shell
   dbt seed --full-refresh -s data_atheon_incremental data_atheon_insert_by_replace || exit 1
   ```

9. **Process New Record with Incremental Models:**
   ```shell
   dbt run -m test_atheon_incremental test_atheon_incremental_no_arguments || exit 1
   ```

10. **Process Restated Data with Period Replace Models:**
    ```shell
    dbt run --vars "{is_replace: True, start_date: '2023-01-01', end_date: '2023-01-04'}" -m test_atheon_insert_by_replace_date || exit 1
    dbt run --vars "{is_replace: True, retailer_id: ['retailer_a']}" -m test_atheon_insert_by_replace_retailer || exit 1
    dbt run --vars "{is_replace: True, supplier_id: ['supplier_a']}" -m test_atheon_insert_by_replace_supplier || exit 1
    dbt run --vars "{is_replace: True, product_id: ['product_a2']}" -m test_atheon_insert_by_replace_product || exit 1
    dbt run --vars "{is_replace: True, start_date: '2023-01-01', end_date: '2023-01-03', retailer_id: ['retailer_b'], supplier_id: ['supplier_a', 'supplier_b'], product_id: ['product_a2']}" -m test_atheon_insert_by_replace || exit 1
    ```

11. **Run Tests with Exclusions:**
    ```shell
    dbt test || exit 1
    ```

12. **Stash changes to incremental models:**
    ```shell
    git stash push --include-untracked "$filepath_incremental" "$filepath_original"
    ```