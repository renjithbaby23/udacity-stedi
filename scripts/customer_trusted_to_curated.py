import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1678906229887 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1678906229887",
)

# Script generated for node Curated Customer With Accelerometer Data Filter
CuratedCustomerWithAccelerometerDataFilter_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1678906229887,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CuratedCustomerWithAccelerometerDataFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1678906375612 = DropFields.apply(
    frame=CuratedCustomerWithAccelerometerDataFilter_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1678906375612",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1678906375612,
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
