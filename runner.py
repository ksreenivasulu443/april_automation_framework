import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set,lit
import os
import sys
import getpass
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utility.general_utility import read_config

from utility.read_data import read_file, read_db, read_snowflake
from utility.validation_library import count_check, duplicate_check, records_present_only_in_source, \
    records_present_only_in_target,data_compare,schema_check,null_value_check,uniqueness_check,column_value_reference_check,column_range_check,name_check

project_path = os.getcwd()

postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"

snow_jar = project_path + "/jars/snowflake-jdbc-3.14.3.jar"
#oracle_jar = project_path + "/jars/ojdbc11.jar"
cwd = os.getcwd()
# user = os.environ.get('USER')
# print(user)
result_local_file = cwd+'\Execution_detailed_summary.txt'
print("result_local_file",result_local_file)

if os.path.exists(result_local_file):
    os.remove(result_local_file)

file = open(result_local_file, 'a')
original = sys.stdout
sys.stdout = file


jar_path = snow_jar

spark = SparkSession.builder.master("local[5]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

Out = {
    "validation_Type": [],
    "Source_name": [],
    "target_name": [],
    "Number_of_source_Records": [],
    "Number_of_target_Records": [],
    "Number_of_failed_Records": [],
    "column": [],
    "Status": [],
    "source_type": [],
    "target_type": []
}

print("project_path", project_path)
template_path = project_path + '/config/Master_Test_Template_project1.xlsx'
print(template_path)

test_cases = pd.read_excel(template_path)

print(test_cases)

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name', 'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

validations = validation.collect()

print(validations)

for row in validations:
    print("row", row)
    if row['source_type'] == 'table':
        source = read_db(spark=spark,
                         table=row['source'],
                         database=row['source_db_name'],
                         query_path=row['source_transformation_query_path'],
                         row=row)
    elif row['source_type'] == 'snowflake':
        source = read_snowflake(spark=spark,
                        table=row['source'],
                        database=row['source_db_name'],
                        query=row['source_transformation_query_path'], row=row)


    else:
        source = read_file(type=row['source_type'],
                           file_name=row['source'],
                           spark=spark,
                           row=row,
                           schema=row['source_schema_path'])

    if row['target_type'] == 'table':
        target = read_db(spark=spark,
                         table=row['target'],
                         database=row['target_db_name'],
                         query_path=row['target_transformation_query_path'],
                         row=row)
    elif row['target_type'] == 'snowflake':
        target = read_snowflake(spark=spark,
                        table=row['target'],
                        database=row['target_db_name'],
                        query=row['target_transformation_query_path'], row=row)
    else:
        target = read_file(type=row['target_type'],
                           file_name=row['target'],
                           spark=spark,
                           row=row,
                           schema=row['target_schema_path'])
    source.show()
    target.show()
    print(row['validation_Type'])
    for validation in row['validation_Type']:
        print(validation)
        if validation == 'count_check':
            count_check(source, target,row,Out)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], Out, row)
        elif validation == 'records_present_only_target':
            records_present_only_in_target(source, target, row['key_col_list'], Out, row)
        elif validation == 'duplicate':
            duplicate_check(target,row['key_col_list'],row,Out)
        elif validation == 'data_compare':
            data_compare(source, target, row['key_col_list'], Out, row)
        elif validation == 'schema_check':
            schema_check(source, target, spark, Out, row)
            #call schema check function
        elif validation == 'null_check':
            null_value_check(target, row['null_col_list'], Out, row)
        elif validation == 'uniqueness_check':
            uniqueness_check(target,row['unique_col_list'],Out, row)
        else:
            print("validation is not defined")


print(Out)

summary = pd.DataFrame(Out)

print(summary)

summary.to_csv("summary.csv")

summary.to_csv("summary.csv")
# summary['bathc_id'] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
schema = StructType([
    StructField("validation_Type", StringType(), True),
    StructField("Source_name", StringType(), True),
    StructField("target_name", StringType(), True),
    StructField("Number_of_source_Records", StringType(), True),
    StructField("Number_of_target_Records", StringType(), True),
    StructField("Number_of_failed_Records", StringType(), True),
    StructField("column", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("target_type", StringType(), True)
])

# Convert Pandas DataFrame to Spark DataFrame
summary = spark.createDataFrame(summary, schema=schema)

system_user = getpass.getuser()

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")



summary = summary.withColumn('batch_id',lit(batch_id))
config_data = read_config('snowflake_db')



summary.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", config_data['jdbc_url']) \
    .option("dbtable", "SAMPLEDB.CONTACT_INFO.SUMMARY") \
    .save()
