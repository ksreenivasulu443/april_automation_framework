from utility.general_utility import read_schema, fetch_file_path


# def read_file(type,path, spark):
#
#     if type == 'csv':
#         df= spark.read.format('csv').load(path)
#     elif type == 'json':
#         df = spark.read.format('json').load(path)
#     elif type == 'parquet':
#         df = spark.read.format('parquet').load(path)
#     elif type == 'avro':
#         df = spark.read.format('avro').load(path)
#     elif type == 'txt':
#         df = spark.read.format('text').load(path)
#     else:
#         print("provided type is not handled")
#
#     return df

def read_file(type,
              file_name,
              spark,
              row,
              schema='NOT APPL',
              multiline=True,
              ):
    try:
        path = fetch_file_path(file_name)
        type = type.lower()
        if type == 'csv':
            if schema != 'NOT APPL':

                schema_json = read_schema(schema)
                print(schema_json)
                print(path)
                df = spark.read.schema(schema_json).option("header", True).option("delimiter", ",").csv(path)
                df.show()
            else:
                df = (spark.read.option("inferSchema", True).
                      option("header", True).option("delimiter", ",").csv(path))
        elif type == 'json':
            if multiline == True:
                df = spark.read.option("multiline", True).json(path)
                df = flatten(df)
            else:
                df = spark.read.option("multiple", False).json(path)
                df = flatten(df)
        elif type == 'parquet':
            df = spark.read.parquet(path)
        elif type == 'avro':
            df = spark.read.format('avro').load("path")
        elif type == 'text':
            df = spark.read.format("text").load(path)
        elif type == 'orc':
            pass
        elif type == 'xyz':
            pass
        else:
            raise ValueError("Unsupported file format", type)
        exclude_cols = row['exclude_columns'].split(',')
        return df.drop(*exclude_cols)
    except FileNotFoundError as e:
        df = None

    except Exception as e:
        df = None
