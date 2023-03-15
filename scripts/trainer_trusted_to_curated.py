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

# Script generated for node Step trainer trusted
Steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1678914556629 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1678914556629",
)

# Script generated for node Trainer and Accelerometer Join
TrainerandAccelerometerJoin_node2 = Join.apply(
    frame1=Steptrainertrusted_node1,
    frame2=Accelerometertrusted_node1678914556629,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="TrainerandAccelerometerJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1678914876949 = DropFields.apply(
    frame=TrainerandAccelerometerJoin_node2,
    paths=["user", "timestamp"],
    transformation_ctx="DropFields_node1678914876949",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1678914876949,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
