import os
import json
from pyspark.sql.types import StructType

given_path = os.path.abspath(os.path.dirname(__file__))

print(given_path)
path = os.path.dirname(given_path)

print(path)

def read_schema(schema_file_path):
    path = os.path.dirname(given_path) + '/schema/' +schema_file_path
    # Read the JSON configuration file
    with open(path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema

def fetch_file_path(file_name):
    path = os.path.dirname(given_path) + '/source_files/'+file_name
    return path

print(read_schema('contact_info_schema.json'))