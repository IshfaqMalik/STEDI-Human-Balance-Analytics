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

# Script generated for node customer_landing
customer_landing_node1734013650117 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1734013650117")

# Script generated for node SQL Query
SqlQuery1093 = '''
select * from cl
where cl.sharewithresearchasofdate <> 0
'''
SQLQuery_node1734105657243 = sparkSqlQuery(glueContext, query = SqlQuery1093, mapping = {"cl":customer_landing_node1734013650117}, transformation_ctx = "SQLQuery_node1734105657243")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734105657243, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734009757104", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1734014072256 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734105657243, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/customer/trusted/", "partitionKeys": []}, transformation_ctx="customer_trusted_node1734014072256")

job.commit()