from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HDFS to PostgreSQL") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# Step 1: Read data from HDFS
hdfs_path = "hdfs://namenode:9000/user/root/data/data.csv"  # Adjust this path to your HDFS file
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Handle missing values
df_cleaned = df.dropna(subset=["SaleAmount", "PropertyType", "Town"])

# Filter invalid rows (e.g., negative sale amounts)
df_cleaned = df_cleaned.filter((col("SaleAmount") > 0) & (col("AssessedValue") >= 0))

# Standardize date format
df_transformed = df_cleaned.withColumn("SaleDate", to_date(col("SaleDate"), "MM/dd/yyyy"))

# Remove Unnecessary Columns
columns_to_remove = ["Non Use Code", "Assessor Remarks", "OPM Remarks", "Location"]
df = df.drop(*columns_to_remove)

# Categorize property types (example: residential, commercial, etc.)
df_transformed = df_transformed.withColumn(
    "PropertyCategory",
    when(col("PropertyType").like("%Residential%"), "Residential")
    .when(col("PropertyType").like("%Commercial%"), "Commercial")
    .otherwise("Other")
)
# Aggregate data
df_aggregated = df_transformed.groupBy("Year").agg({"SaleAmount": "sum"})

# Step 2: Write the DataFrame to PostgreSQL
jdbc_url = "jdbc:postgresql://postgresDB:5432/postgres"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df.write \
    .jdbc(url=jdbc_url, table="public.realEstate", mode="overwrite", properties=properties)
print("Data has been pushed to PostgreSQL successfully!")

# Stop SparkSession
spark.stop()
