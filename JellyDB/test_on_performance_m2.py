from JellyDB.db import Database
from JellyDB.query import Query

from random import choice, randint, sample, seed
import os,sys
from time import process_time
#size_to_test = int(sys.argv[2])
#os.sys('rm ~/ECS165/*')
def performance_m2(size_to_test):
    db = Database()
    db.open('~/ECS165')
    # Student Id and 4 grades
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    records = {}
    seed(3562901)
    for i in range(0, size_to_test):
        key = 92106429 + i
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    keys = sorted(list(records.keys()))
    print("Insert finished")

    time_select_1_s = process_time()
    for key in keys:
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            print('select error on', key, ':', record, ', correct:', records[key])
        # else:
        #     print('select on', key, ':', record)
    time_select_1_f = process_time()
    print("Select finished")

    time_update_s = process_time()
    for _ in range(10):
        for key in keys:
            updated_columns = [None, None, None, None, None]
            for i in range(1, grades_table.num_columns):
                value = randint(0, 20)
                updated_columns[i] = value
                original = records[key].copy()
                records[key][i] = value
                query.update(key, *updated_columns)
                record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                error = False
                for j, column in enumerate(record.columns):
                    if column != records[key][j]:
                        error = True
                if error:
                    print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
                #else:
                    #print('update on', original, 'and', updated_columns, ':', record)
                updated_columns[i] = None
    time_update_f = process_time()
    print("Update finished")
    for i in range(0, 100):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], 0)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        # else:
        #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
    print("Aggregate finished")

    db.close()

    # Student Id and 4 grades
    db = Database()
    db.open('~/ECS165')
    grades_table = db.get_table('Grades')
    query = Query(grades_table)

    # repopulate with random data
    records = {}
    seed(3562901)
    for i in range(0, size_to_test):
        key = 92106429 + i
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    keys = sorted(list(records.keys()))
    for _ in range(10):
        for key in keys:
            for j in range(1, grades_table.num_columns):
                value = randint(0, 20)
                records[key][j] = value
    keys = sorted(list(records.keys()))
    for key in keys:
        print(records[key])
        print(records[key])

    time_select_2_s = process_time()
    for key in keys:
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            print('select error on', key, ':', record, ', correct:', records[key])
    time_select_2_f = process_time()
    print("Select finished")

    deleted_keys = sample(keys, 100)
    for key in deleted_keys:
        query.delete(key)
        records.pop(key, None)

    for i in range(0, 100):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], 0)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    print("Aggregate finished")
    db.close()
    return ['8192',time_update_f-time_update_s,time_select_1_f-time_select_1_s,time_select_2_f-time_select_2_s]
