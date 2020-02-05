from JellyDB.db import Database
from JellyDB.query import Query
from time import process_time
from random import choice, randint, sample, seed
from colorama import Fore, Back, Style

# Student Id and 4 grades


#seed(3562901)
def performance_testing(range_to_test):
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    records = {}
    insert_time_0 = process_time()
    for i in range(0, range_to_test):
        key = 92106429 + randint(0, 9000)
        while key in records:
            key = 92106429 + randint(0, 9000)
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    insert_time_1 = process_time()
    time_insert = insert_time_1 - insert_time_0
    print('inserted', range_to_test, 'records took:  \t\t\t', time_insert)

    select_time_0 = process_time()
    for key in records:
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            print('select error on', key , ':', record, ', correct:', records[key])
    select_time_1 = process_time()
    time_select = select_time_1 - select_time_0
    print('select',range_to_test, 'records took:  \t\t\t', time_select)

    update_time_0 = process_time()
    for key in records:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, [1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
            updated_columns[i] = None
    update_time_1 = process_time()
    time_update = update_time_1 - update_time_0
    print('updated', range_to_test,'records took:  \t\t\t', time_update)

    keys = sorted(list(records.keys()))
    agg_time_0 = process_time()
    for c in range(0, grades_table.num_columns):
        for i in range(0, 20):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], c)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    agg_time_1 = process_time()
    time_sum = agg_time_1 - agg_time_0
    print('Sum',range_to_test,'records took:  \t\t\t', time_sum)
    
    delete_time_0 = process_time()
    for key in records:
        query.delete(key)
    delete_time_1 = process_time()
    time_delete= delete_time_1 - delete_time_0
    print('Deletion',range_to_test,'records took:  \t\t\t', time_delete)
    return time_insert,time_select,time_update,time_sum,time_delete
