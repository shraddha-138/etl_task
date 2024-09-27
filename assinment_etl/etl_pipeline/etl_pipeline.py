from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when ,current_timestamp
from pyspark.sql.window import Window


def get_spark_session() -> SparkSession:
    """Initialize and return a SparkSession."""
    return SparkSession.builder \
        .appName("index_practice") \
        .config("spark.jars", "/home/shraddha/Downloads/hudi-spark3.5-bundle_2.12-0.15.0.jar") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read() -> DataFrame:
    """Read the file into a DataFrame."""
    spark = get_spark_session()
    df = spark.read.csv("/home/shraddha/Downloads/014-Data.csv", header=True, inferSchema=True)
    df = df.withColumn("ts", current_timestamp())
    df.createOrReplaceTempView("source_data")
    return df

def fill_nulls(df: DataFrame) -> DataFrame:
    """Fill null values in the DataFrame."""
    fill_values = {
        'CustomerKey': 'Unknown',
        'CustomerName': 'Unknown',
        'OrderDate': '1900-01-01',
        'Color': 'Unknown'
    }
    return df.na.fill(fill_values)

def calculate_running_total(df: DataFrame) -> DataFrame:
    """Calculate the running total of SalesAmount."""
    window_spec = Window.partitionBy('Country').orderBy('OrderDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn('RunningTotal', spark_sum(col('SalesAmount')).over(window_spec))
    df.createOrReplaceTempView("transformed_data")
    return df

def write_to_hudi(df: DataFrame, hudi_path: str) -> None:
    """Write the DataFrame to Apache Hudi."""
    hudi_options = {
        "hoodie.table.name": "hudi_table",
        "hoodie.datasource.write.recordkey.field": "ProductKey",
        "hoodie.datasource.write.partitionpath.field": "Country",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.index.type": "GLOBAL_BLOOM",
        "hoodie.index.global.bloom.filter": "true"
    }
    df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)

def etl_pipeline():
    df = read()
    df = fill_nulls(df)
    df = calculate_running_total(df)
    write_to_hudi(df, hudi_path="/home/shraddha/data_csv3")
