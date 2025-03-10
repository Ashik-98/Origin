import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# S3 Configurations
s3_bucket = "awsashik123bucket"
s3_prefix = "Health_project/"
output_path = "s3://awsashik123bucket/Output_data/parquet/"
manifest_path = "s3://awsashik123bucket/manifest/processed_files.txt"

# Initialize Boto3 S3 Client
s3_client = boto3.client("s3")
# Read previously processed files from the manifest
try:
    response = s3_client.get_object(Bucket=s3_bucket, Key="manifest/processed_files.txt")
    processed_files = set(response["Body"].read().decode().splitlines())
except s3_client.exceptions.NoSuchKey:
    processed_files = set()

print("previously processed files:",processed_files)

# List files in S3
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
all_files = [obj["Key"] for obj in response.get("Contents", []) if not obj["Key"].endswith("/")]
print("all files:",all_files)

# Identify new files
new_files = [file for file in all_files if file not in processed_files]
print(f"New files to process: {new_files}")
# Dictionary to store DataFrames
dynamic_frames = {}

# Read only new files
for index, file_key in enumerate(new_files):
    s3_path = f"s3://{s3_bucket}/{file_key}"
    print(f"Reading file: {s3_path}")

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path]},
        format="csv",
        format_options={"withHeader": True}
    )

    dynamic_frames[f"df_{index}"] = dynamic_frame

# If no new data, exit gracefully
if not dynamic_frames:
    print("No new data to process.")
    sys.exit(0)

# Convert DynamicFrames to DataFrames
medical_updated_df = dynamic_frames["df_0"].toDF() if "df_0" in dynamic_frames else None
patients_updated_df = dynamic_frames["df_1"].toDF() if "df_1" in dynamic_frames else None
patients_updated_df = patients_updated_df.withColumn(
    "admission_date",
    F.when(patients_updated_df["patient_id"] == "P013", None).otherwise(patients_updated_df["admission_date"])
)
patients_updated_df = patients_updated_df.withColumn(
    "admission_date",
    F.when(patients_updated_df["patient_id"] == "P012", '8/22/2024').otherwise(patients_updated_df["admission_date"])
)
patients_updated_df = patients_updated_df.withColumn(
    "patient_id",
    F.when(F.col("name") == "Ava Johnson", "P008").otherwise(F.col("patient_id"))
)
print("patients table updated:")
patients_updated_df.show()
print("medical table updated:")
medical_updated_df.show()
# Data Cleaning
patients_updated_df = patients_updated_df.dropDuplicates().na.fill("Unknown").orderBy('patient_id')
patients_updated_df.show()
# Incremental Processing

joined_updated_df = patients_updated_df.join(medical_updated_df, "patient_id", "inner")

windowfn = Window.partitionBy("doctor_id").orderBy("record_id")
joined_updated_df = joined_updated_df.withColumn("doctor_test_counts", F.count("record_id").over(windowfn))
#joined_updated_df.show()

abnormal_tests = medical_updated_df.withColumn(
    "is_abnormal", F.when(F.col("test_results") != "Normal", 1).otherwise(0)
)

abnormal_percentage = abnormal_tests.groupBy("doctor_id").agg(
    (F.sum("is_abnormal") / F.count("record_id") * 100).alias("abnormal_percentage")
)

final_updated_df = joined_updated_df.join(abnormal_percentage, "doctor_id", "left")
#final_updated_df.show(truncate = False)

# Merge new data with existing data in S3
try:
    existing_df = spark.read.parquet(output_path)
    print("Existing_df:")
    existing_df.show(truncate = False)
    merged_df = existing_df.union(final_updated_df).dropDuplicates(["record_id"])
    print("Merged_final_df:")
    merged_df.show(truncate = False)
except Exception as e:
    print(f"No existing data found, writing new data. Error: {str(e)}")
    #merged_df = final_updated_df
# Write merged data
merged_df.write.format("parquet").mode("overwrite").option("path","s3://awsashik123bucket/Output_data/new/").save()

# Update manifest with processed files
#s3_client.put_object(Bucket=s3_bucket, Key="manifest/processed_files.txt", Body="\n".join(new_files))

print("Incremental load completed successfully.")

job.commit()