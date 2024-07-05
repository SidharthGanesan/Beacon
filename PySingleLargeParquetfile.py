from pyspark.sql import SparkSession

def create_single_parquet_file(storage_account_name, container_name, input_directory, output_directory, access_key):
    # Configure Spark to access Azure Data Lake Storage Gen2
    spark = SparkSession.builder.appName("SingleParquetFileCreation") \
        .config("fs.azure.account.auth.type", "SharedKey") \
        .config("fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net", access_key) \
        .getOrCreate()
    
    # Define the input and output paths
    input_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{input_directory}"
    output_file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{output_directory}/single_large_parquet_file.parquet"
    
    # Read the Parquet files from Azure Data Lake
    df = spark.read.parquet(input_path)
    
    # Repartition the DataFrame to have a single partition
    df_single_partition = df.repartition(1)
    
    # Write the repartitioned DataFrame back to Azure Data Lake in Parquet format
    df_single_partition.write.mode('overwrite').parquet(output_file_path)

# Example usage
storage_account_name = 'your_storage_account_name'
container_name = 'your_container_name'
input_directory = 'input/parquet_files'
output_directory = 'output/single_large_file'
access_key = 'your_storage_account_access_key'

create_single_parquet_file(storage_account_name, container_name, input_directory, output_directory, access_key)