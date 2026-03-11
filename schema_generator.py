import duckdb

def get_parquet_schema(jsonl_path):
    # Connect to in-memory DuckDB
    con = duckdb.connect(':memory:')
    
    # We use DESCRIBE to get column names and their inferred Parquet types and sample_size=-1 to ensure we scan the entire file for accurate schema inference
    schema_df = con.execute(f"DESCRIBE SELECT * FROM read_json_auto('{jsonl_path}', sample_size=-1)").df()
    print(schema_df)
   
    # Convert to the specific dictionary format DuckDB expects
    # Example: {'id': 'BIGINT', 'name': 'VARCHAR'}
    schema_dict = dict(zip(schema_df['column_name'], schema_df['column_type']))
    return schema_dict

# Example Usage
parquet_schema = get_parquet_schema("entries_test.json")
