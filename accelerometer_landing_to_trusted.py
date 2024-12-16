import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node customer_trusted
customer_trusted_node1734018787152 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1734018787152")

# Script generated for node acclerometer_landing
acclerometer_landing_node1734018832767 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/accelerometer/landing/"], "recurse": True}, transformation_ctx="acclerometer_landing_node1734018832767")

# Script generated for node SQL Query
SqlQuery1049 = '''
select al.user, al.timestamp, 
al.x, al.y, al.z from al 
join ct on al.user = ct.email
'''
SQLQuery_node1734107740541 = sparkSqlQuery(glueContext, query = SqlQuery1049, mapping = {"ct":customer_trusted_node1734018787152, "al":acclerometer_landing_node1734018832767}, transformation_ctx = "SQLQuery_node1734107740541")

# Script generated for node Drop Duplicates
DropDuplicates_node1734108193525 =  DynamicFrame.fromDF(SQLQuery_node1734107740541.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734108193525")

# Script generated for node acclerometer_trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1734108193525, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734018604491", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
acclerometer_trusted_node1734019105589 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1734108193525, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="acclerometer_trusted_node1734019105589")

job.commit()