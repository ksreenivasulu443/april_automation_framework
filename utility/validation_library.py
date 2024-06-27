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
    print("*" * 50)
    #print(f"{validation} has started")
    print("*" * 50)
    key_column = key_column.split(",")-'col1,col2' --> ['col1','col2']
    duplicate = target.groupBy(key_column).count().where('count>1')
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
    print("*" * 50)
    #print(f"{validation} has been completed")
    print("*" * 50)



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