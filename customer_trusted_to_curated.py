import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1734019833232 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1734019833232")

# Script generated for node customer_trusted
customer_trusted_node1734019503205 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1734019503205")

# Script generated for node Join
Join_node1734020328122 = Join.apply(frame1=customer_trusted_node1734019503205, frame2=accelerometer_trusted_node1734019833232, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1734020328122")

# Script generated for node Drop Fields
DropFields_node1734020346846 = DropFields.apply(frame=Join_node1734020328122, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1734020346846")

# Script generated for node Drop Duplicates
DropDuplicates_node1734168284201 =  DynamicFrame.fromDF(DropFields_node1734020346846.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734168284201")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1734168284201, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734019463675", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1734020372244 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1734168284201, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/customer/curated/", "partitionKeys": []}, transformation_ctx="customer_curated_node1734020372244")

job.commit()