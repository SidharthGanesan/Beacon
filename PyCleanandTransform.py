from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, unix_timestamp

def process_csv_files_in_azure_datalake(storage_account_name, container_name, input_directory, output_directory, access_key):
    # Configure Spark to access Azure Data Lake Storage Gen2
    spark = SparkSession.builder.appName("AzureDatalakeIntegration") \
        .config("fs.azure.account.auth.type", "SharedKey") \
        .config("fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net", access_key) \
        .getOrCreate()
    
    # Define the input and output paths
    input_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{input_directory}"
    output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{output_directory}"
    
    # Read the CSV files from Azure Data Lake
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Process the DataFrame (example transformations)
    if 'ticker' in df.columns:
        df = df.withColumn('ticker', col('ticker').cast('string'))
    if 'Date' in df.columns:
        df = df.withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd'))
    if 'Time' in df.columns:
        df = df.withColumn('Time', unix_timestamp(col('Time'), 'HH:mm:ss').cast('timestamp'))
    if 'LTP' in df.columns:
        df = df.withColumn('LTP', col('LTP').cast('string'))
    if 'BuyPrice' in df.columns:
        df = df.withColumn('BuyPrice', col('BuyPrice').cast('float'))
    if 'BuyQty' in df.columns:
        df = df.withColumn('BuyQty', col('BuyQty').cast('integer'))
    if 'SellPrice' in df.columns:
        df = df.withColumn('SellPrice', col('SellPrice').cast('float'))
    if 'SellQty' in df.columns:
        df = df.withColumn('SellQty', col('SellQty').cast('integer'))
    if 'LTQ' in df.columns:
        df = df.withColumn('LTQ', col('LTQ').cast('string'))
    if 'OpenInterest' in df.columns:
        df = df.withColumn('OpenInterest', col('OpenInterest').cast('string'))
    if 'timestamp' in df.columns:
        df = df.withColumn('timestamp', to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss'))

    df.write.mode('overwrite').parquet(output_path)

# Example usage
storage_account_name = 'TrueBeaconDatalake'
container_name = 'Stockequitylevel2data'
input_directory = 'input/csv_files'
output_directory = 'output/parquet_files'
access_key = 'your_storage_account_access_key'

process_csv_files_in_azure_datalake(storage_account_name, container_name, input_directory, output_directory, access_key)