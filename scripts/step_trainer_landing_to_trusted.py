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

# Script generated for node Step trainer landing
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-3/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Customer curated
Customercurated_node1678911275535 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="Customercurated_node1678911275535",
)

# Script generated for node Join StepTrainer with curated customer
JoinStepTrainerwithcuratedcustomer_node2 = Join.apply(
    frame1=Steptrainerlanding_node1,
    frame2=Customercurated_node1678911275535,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinStepTrainerwithcuratedcustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1678912485832 = DropFields.apply(
    frame=JoinStepTrainerwithcuratedcustomer_node2,
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
    transformation_ctx="DropFields_node1678912485832",
)

# Script generated for node Step trainer Trusted
SteptrainerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1678912485832,
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="SteptrainerTrusted_node3",
)

job.commit()
