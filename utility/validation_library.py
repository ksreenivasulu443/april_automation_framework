from pyspark.sql.functions import count,col, when,upper, isnan
def count_check(source, target,row,Out):
    src_cnt = source.count()
    tgt_cnt = target.count()
    if src_cnt == tgt_cnt :
        write_output(validation_Type='count_check',
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=src_cnt-tgt_cnt,
                     column=row['key_col_list'],
                     Status='Pass',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)

    else:
        write_output(validation_Type='count_check',
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=src_cnt - tgt_cnt,
                     column=row['key_col_list'],
                     Status='Fail',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)



def duplicate_check(target, key_column, row, Out):
    key_column = key_column.split(",")
    duplicate = target.groupBy(key_column).count().where('count>1')
    #select col1,col2, count(1) from tn group by col1,col2 having count(1)>1
    duplicate.show()
    target_count = target.count()
    failed = duplicate.count()
    if failed > 0:
        print("Duplicates present")
        write_output(validation_Type='duplicate', source='NOT APPL', target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        print("duplicates not present")
        write_output(validation_Type='duplicate', source='NOT APPL', target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)

def uniqueness_check(target: str,
                     unique_col_list: list,
                     Out: dict,
                     row: dict):

    unique_col_list = unique_col_list.split(",")
    target_count = target.count()
    for col in unique_col_list:
        unique_values = target.groupBy(col).count().where('count>1')
        unique_values.show()

        failed = unique_values.count()

        if failed > 0:
            print("duplicates present on ", col)
            write_output(validation_Type='uniqueness_check', source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=failed, column=col, Status='FAIL',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)
        else:
            print("Duplicates not present", col)
            write_output(validation_Type='uniqueness_check', source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=0, column=col, Status='PASS',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)

def records_present_only_in_target(source, target, keyList: str, Out, row):
    columns = keyList
    keyList = keyList.split(",")
    tms = target.select(keyList).exceptAll(source.select(keyList))
    failed_records = tms
    failed = tms.count()
    print("records_present_only_in_target and not source :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    failed_records.show()
    if failed > 0:
        write_output(validation_Type="records_present_only_in_target", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="records_present_only_in_target", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)

def records_present_only_in_source(source, target, keyList, Out, row):
    columns = keyList
    keyList = keyList.split(",")
    smt = source.select(keyList).exceptAll(target.select(keyList))
    failed_records = smt
    failed = smt.count()
    print("records_present_only_in_source not in target :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    failed_records.show()
    if failed > 0:
        write_output(validation_Type="records_present_only_in_source", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="records_present_only_in_source", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)

def null_value_check(target, Null_columns, Out, row):
    target_count = target.count()
    Null_columns = Null_columns.split(",")
    for column in Null_columns:
        Null_df = target.select(count(when(col(column).contains('None') |
                                           upper(col(column)).contains('NULL') |
                                           upper(col(column)).contains('NA') |
                                           (col(column) == '') |
                                           col(column).isNull() |
                                           isnan(column), column
                                           )).alias("Null_value_count"))

        Null_df.show()
        failed = Null_df.collect()[0][0]

        if failed > 0:
            write_output(validation_Type="null_value_check", source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=failed, column=column, Status='FAIL',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)
        else:
            write_output(validation_Type="null_value_check", source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=0, column=column, Status='PASS',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)


def write_output(validation_Type, source, target, Number_of_source_Records, Number_of_target_Records,
                 Number_of_failed_Records, column, Status, source_type, target_type, Out):
    Out["Source_name"].append(source)
    Out["target_name"].append(target)
    Out["column"].append(column)
    Out["validation_Type"].append(validation_Type)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)
    Out["source_type"].append(source_type)
    Out["target_type"].append(target_type)