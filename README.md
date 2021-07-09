# sqlserver-data-generator
Free python generator for generation of test data for databases

### Using
  1. Clone repository
  2. Open file "sqlserver-data-generator.py" 
  3. Set values for the following variables in MAIN block: 
    - server_name
    - db_name
    - schema_name
    - table_names
    - rows_count
    - json_file_path
    - sql_file_path
  4. Run script

### Results
  1. JSON file that contains structure of selected tables (default, "output/table_info.json")
  2. SQL file that contains script for inserting generated data into selected tables (default, "output/{db_name}_test_data_population.sql")
