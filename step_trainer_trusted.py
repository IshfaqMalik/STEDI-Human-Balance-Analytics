import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Amazon S3
AmazonS3_node1734452562338 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1734452562338")

# Script generated for node customer_curated
customer_curated_node1734452509077 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1734452509077")

# Script generated for node SQL Query
SqlQuery107 = '''
SELECT st.serialnumber, 
       st.sensorreadingtime, 
       st.distancefromobject
FROM st
JOIN cc 
ON st.serialnumber = cc.serialnumber

'''
SQLQuery_node1734452637521 = sparkSqlQuery(glueContext, query = SqlQuery107, mapping = {"st":AmazonS3_node1734452562338, "cc":customer_curated_node1734452509077}, transformation_ctx = "SQLQuery_node1734452637521")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734452637521, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734452502180", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1734452667518 = glueContext.getSink(path="s3://stedi-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1734452667518")
step_trainer_trusted_node1734452667518.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1734452667518.setFormat("json")
step_trainer_trusted_node1734452667518.writeFrame(SQLQuery_node1734452637521)
job.commit()