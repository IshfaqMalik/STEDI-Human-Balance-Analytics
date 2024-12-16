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

# Script generated for node step_trainer
step_trainer_node1734170224994 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_node1734170224994")

# Script generated for node customer_curated
customer_curated_node1734170167264 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1734170167264")

# Script generated for node SQL Query
SqlQuery1007 = '''
SELECT st.serialnumber, 
       st.sensorreadingtime, 
       st.distancefromobject
FROM st
JOIN cc 
ON st.serialnumber = cc.serialnumber
'''
SQLQuery_node1734170609507 = sparkSqlQuery(glueContext, query = SqlQuery1007, mapping = {"cc":customer_curated_node1734170167264, "st":step_trainer_node1734170224994}, transformation_ctx = "SQLQuery_node1734170609507")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734170609507, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734168958018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1734170759989 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734170609507, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1734170759989")

job.commit()