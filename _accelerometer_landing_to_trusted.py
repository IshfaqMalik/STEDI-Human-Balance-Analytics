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

# Script generated for node accelerometer_landing
accelerometer_landing_node1734450594500 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1734450594500")

# Script generated for node customer_trusted
customer_trusted_node1734450716693 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1734450716693")

# Script generated for node privacy_filter
SqlQuery101 = '''
select al.user, al.timestamp, 
al.x, al.y, al.z from al 
join ct on al.user = ct.email

'''
privacy_filter_node1734450863301 = sparkSqlQuery(glueContext, query = SqlQuery101, mapping = {"al":accelerometer_landing_node1734450594500, "ct":customer_trusted_node1734450716693}, transformation_ctx = "privacy_filter_node1734450863301")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=privacy_filter_node1734450863301, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734449521216", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1734450955501 = glueContext.getSink(path="s3://stedi-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1734450955501")
accelerometer_trusted_node1734450955501.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1734450955501.setFormat("json")
accelerometer_trusted_node1734450955501.writeFrame(privacy_filter_node1734450863301)
job.commit()