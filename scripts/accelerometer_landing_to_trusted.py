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

# Script generated for node CustomerTrusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1678898437134 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1678898437134",
)

# Script generated for node Customer Trusted Join
CustomerTrustedJoin_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1678898437134,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerTrustedJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1678899874862 = DropFields.apply(
    frame=CustomerTrustedJoin_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1678899874862",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1678899874862,
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrustedZone_node3",
)

job.commit()
