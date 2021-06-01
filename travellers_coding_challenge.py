from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType, StringType
import pyspark.sql.functions as f
from datetime import datetime, timedelta


def get_spark_session():
    """
    :return: return a configured spark session
    :rtype: SparkSession
    """
    return (
        SparkSession
            .builder
            .enableHiveSupport()
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.driver.host", "10.51.4.110")
            .config("spark.driver.bindAddress", "10.51.4.110")
            .appName("Travellers_coding_challenge")
            .getOrCreate()
    )

def explode(exp, col_list):
    """
    Args: this method is used to convert multi nested JSON a dataframe columns based on the datatype sequence identified in the columns
        exp: exp to select columns and explode
        col_list: columns list to iterate based on data types

    Returns:

    """
    i = 1
    while(True):
        for name in exp.dtypes:
            name = name[0]
            if name in col_list:
                exp = exp.withColumnRenamed(name, f'{name}_c{i}')
        cols = [c[0] for c in exp.dtypes if c[1][:5] != 'array']
        exp = exp.select(*cols, f.explode_outer(f'children_c{i}').alias('children'))
        flat_cols = [c[0] for c in exp.dtypes if c[1][:6] != 'struct']
        if {k:v for k,v in exp.dtypes}['children'] == 'string':
            return exp
        exp = exp.select(*flat_cols, 'children.*')
        i += 1

# creating a spark session
spark = get_spark_session()

#reading input JSON file
df = spark.read.option("MULTILINE", "true").json('/Users/p249063/Downloads/codingchallenge/travellers_coding_challenge')

# converting nested JSON to CSV file
col_list = ['guid',
 'industry',
 'children',
 'is_service_provider',
 'is_subscribed',
 'name',
 'rating',
 'rating_type']
for name in col_list:
    df = df.withColumnRenamed(name, f'{name}_parent')

df.columns

cols = [c[0] for c in df.dtypes if c[1][:5] != 'array']
exp = (df
       .select(*cols, f.explode_outer('children_parent').alias('children'))
       .select(*cols, 'children.*')
      )
final_dataframe = explode(exp, col_list)

#counting the number of output rows and writing the final df in csv format
final_dataframe.count()
final_dataframe.write.csv(path="/Users/pavan/codingchallenge/", mode= "overwrite")



# Exploratory Data Analysis
#scenario 1: Get only the maximum rating for an industry
analysis_df = final_dataframe.select("industry_parent", "industry_c1", "industry_c2", "industry_c3", "rating_parent", "rating_c1", "rating_c2", "rating_c3")
window_max = Window.partitionBy("industry_parent", "industry_c1", "industry_c2", "industry_c3").orderBy(f.col("rating_parent").desc(), f.col("rating_c1").desc(), f.col("rating_c2").desc(), f.col("rating_c3").desc())
analysis_df.withColumn("rankingMax", f.row_number().over(window_max)).filter(f.col("rankingMax") == 1).show()


#Scenario 2 : Get the minimum rating of a child company for the parent company
window_min = Window.partitionBy("industry_parent", "industry_c1", "industry_c2", "industry_c3").orderBy("rating_parent", "rating_c1", "rating_c2", "rating_c3")
analysis_df.withColumn("rankingMin", f.row_number().over(window_min)).filter(f.col("rankingMin") == 1).show()

#Scenario 3 : Transpose all the child companies as columns for a parent company
# Final dataframe itself is transposed so that each row for the child has a parent details


