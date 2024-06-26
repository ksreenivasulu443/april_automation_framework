def count_check(source, target,row):
    src_cnt = source.count()
    tgt_cnt = target.count()
    source_file_table= row['source']
    target_file_table = row['target']
    if src_cnt == tgt_cnt :
        print("count is matching")
    else:
        print(f"count is not matching, source count is {src_cnt}, target count is {tgt_cnt},{source_file_table},{target_file_table}")

def duplicate_check(target, key_column):
    fail = target.groupBy(key_column).count().filter("count>1")