from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def process_parquet_file(storage_account_name, container_name, parquet_directory, bhavcopy_file_path, access_key):
    # Initialize Spark session
    spark = SparkSession.builder.appName("ProcessParquetFile").config("fs.azure.account.auth.type", "SharedKey") \
        .config("fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net", access_key) \
        .getOrCreate()
    
    # Define the input path for the target Parquet file
    parquet_input_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{parquet_directory}"
    
    # Read the target Parquet file
    df = spark.read.parquet(parquet_input_path)
    
    # Filter out records with null or negative values in any column
    filtered_df = df.select(*[col(c).alias(c) for c in df.columns if c not in ['ticker', 'date']])
    filtered_df = filtered_df.select(*['ticker', 'date'] + [col(c).cast("double").alias(c) for c in filtered_df.columns])
    filtered_df = filtered_df.filter(" AND ".join([f"({c} IS NOT NULL AND {c} >= 0)" for c in filtered_df.columns]))
    
    # Read the "bhavcopy" dataset
    bhavcopy_df = spark.read.parquet(bhavcopy_file_path)
    
    # Filter for unique ticker IDs based on the "bhavcopy" dataset
    unique_tickers_df = filtered_df.join(bhavcopy_df, filtered_df.ticker == bhavcopy_df.ticker, "inner").select(filtered_df["*"]).distinct()
    
    # Aggregate records by 'tickers' and 'date' columns
    aggregated_df = unique_tickers_df.groupBy("ticker", "date").agg(count('BuyPrice','SellPrice'))
    
    # Process the resulting DataFrame as needed (e.g., show or save)
    aggregated_df.show()
    aggregated_df.write.mode('overwrite').parquet(output_file_path) 

# Example usage
storage_account_name = 'your_storage_account_name'
container_name = 'your_container_name'
parquet_directory = 'input/parquet_files'
bhavcopy_file_path = '/path/to/your/bhavcopy_file.parquet'
access_key = 'your_storage_account_access_key'
output_file_path = 'Output file path'
process_parquet_file(storage_account_name, container_name, parquet_directory, bhavcopy_file_path, access_key)