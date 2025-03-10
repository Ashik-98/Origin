
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 Configurations
s3_bucket = "awsashik123bucket"
s3_prefix = "Health_project/"  # Folder where files are stored

# Initialize Boto3 S3 Client
s3_client = boto3.client("s3")

# List all files in the S3 folder
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
#print(response.get("Contents"))

# Extract file paths (excluding folders)
file_keys = [obj["Key"] for obj in response.get("Contents", []) if not obj["Key"].endswith("/")]
print(file_keys)

# Dictionary to store multiple DynamicFrames
dynamic_frames = {}

# Read each file into a separate Glue DynamicFrame
for index, file_key in enumerate(file_keys):
    s3_path = f"s3://{s3_bucket}/{file_key}"
    print(f"Reading file: {s3_path}")

    # Create a separate Glue DynamicFrame for each file
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path]},
        format="csv",  # Change format if needed (json, parquet, etc.)
        format_options={"withHeader": True}
    )

    # Store DynamicFrame in a dictionary
    dynamic_frames[f"df_{index}"] = dynamic_frame

    # Print schema of each file
    print(f"Schema of {file_key}:")
    dynamic_frame.printSchema()

# Example: Convert first DynamicFrame to DataFrame and show
if dynamic_frames:
    medical_df = dynamic_frames["df_0"].toDF()
    patients_df = dynamic_frames["df_1"].toDF()
    patients_df.show()
    medical_df.show()

print("Glue Job Completed Successfully")
patients_df.show()
# Initialize Spark
# spark = SparkSession.builder.appName("HealthcareETL").getOrCreate()

# # Load raw patient data
# df_patients = spark.read.csv("s3://healthcare-bucket/raw/patients.csv", header=True, inferSchema=True)


# Data Cleaning
#patients_df = patients_df.dropDuplicates().fillna({'diagnosis': 'Unknown'})

joined_df = patients_df.join(medical_df, "patient_id", "inner")

#joined_df.show()
windowfn = Window.partitionBy("doctor_id").orderBy("record_id")
joined_df = joined_df.withColumn("doctor_test_counts", F.count("record_id").over(windowfn))
#joined_df.show()


# windowfn2 = Window.partitionBy("diagnosis").orderBy("patient_id")
# joined_df = joined_df.withColumn("diagnosis_count", F.count("patient_id").over(windowfn2))
# #joined_df.show()


# Aggregate: Count abnormal tests
abnormal_tests = medical_df.withColumn(
    "is_abnormal",
    F.when(F.col("test_results") != "Normal", 1).otherwise(0)
)

#abnormal_tests.show()

abnormal_percentage = abnormal_tests.groupBy("doctor_id").agg(
    (F.sum(F.col("is_abnormal")) / F.count("record_id") * 100).alias("abnormal_percentage")
)
#abnormal_percentage.show()

final_df = joined_df.join(abnormal_percentage, "doctor_id", "left").orderBy("patient_id")
final_df.show(truncate = False)




# Write to Delta Lake (Parquet format)
final_df.write.format("parquet").mode("overwrite").save("s3://awsashik123bucket/Output_data/parquet/")
job.commit()