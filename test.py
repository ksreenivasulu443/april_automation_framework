from pyspark.sql import SparkSession

from pyspark.sql.functions import sha2,concat,col,lit,length

from utility.read_data import read_file

from utility.general_utility import read_schema
spark = SparkSession.builder.master("local[5]") \
    .appName("test") \
    .getOrCreate()

df = spark.read.csv(r"/Users/harish/PycharmProjects/april_automation_framework/source_files/Contact_info.csv", inferSchema=True, header=True)
df.printSchema()

# df.schema.json()
#
# schema_json = read_schema('test_schema.json')
#
# df2 = spark.read.schema(schema_json).option("header", True).option("delimiter", ",").csv(r"/Users/harish/PycharmProjects/april_automation_framework/source_files/Contact_info.csv")
#



failed2 = df.withColumn("hash_key",length(sha2(concat('Identifier','Surname'), 384)))

failed2.show()
