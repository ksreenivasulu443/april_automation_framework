from pyspark.sql import SparkSession

from pyspark.sql.functions import sha2,concat,col,lit,length,regexp_extract

from utility.read_data import read_file

from utility.general_utility import read_schema
spark = SparkSession.builder.master("local[5]") \
    .appName("test") \
    .getOrCreate()

df = spark.read.csv(r"/Users/harish/PycharmProjects/april_automation_framework/source_files/Contact_info.csv", inferSchema=True, header=True)
df.show()

# df.schema.json()
#
# schema_json = read_schema('test_schema.json')
#
# df2 = spark.read.schema(schema_json).option("header", True).option("delimiter", ",").csv(r"/Users/harish/PycharmProjects/april_automation_framework/source_files/Contact_info.csv")
#



# failed2 = df.withColumn("hash_key",length(sha2(concat('Identifier','Surname'), 384)))
#
# failed2.show()

# pattern = r"^[a-zA-Z@#]+$"

column ='given name'

pattern = r"^(?:[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|\[IPv6:[a-fA-F0-9:.]+\])\])$"

    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
df2 = df.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")

df2.show()
