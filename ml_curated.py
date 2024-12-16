import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1734172031679 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1734172031679")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1734171979397 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/accelerometer/trusted"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1734171979397")

# Script generated for node Join
Join_node1734172095931 = Join.apply(frame1=step_trainer_trusted_node1734172031679, frame2=accelerometer_trusted_node1734171979397, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1734172095931")

# Script generated for node ML_Curated
EvaluateDataQuality().process_rows(frame=Join_node1734172095931, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734171100953", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ML_Curated_node1734188381961 = glueContext.write_dynamic_frame.from_options(frame=Join_node1734172095931, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/ML_curated/", "partitionKeys": []}, transformation_ctx="ML_Curated_node1734188381961")

job.commit()