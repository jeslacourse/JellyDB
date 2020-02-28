"""
Usage: python -m JellyDB.test
"""
from JellyDB.db import Database
from JellyDB.query import Query
from JellyDB.config import Config
from time import process_time
from random import choice, randrange
import traceback

def get_columns(record_object):
    return record_object[0].columns

def correctness_testing(fav_numbers):
    print("\n====Correctness testing:====\n")
    tests_passed = 0
    tests_failed = 0

    try:
        assert fav_numbers._name == "fav_numbers"
        assert fav_numbers._num_columns == 3 + Config.METADATA_COLUMN_COUNT
        assert fav_numbers._key == 0

        print("fav_numbers create table passed")
        tests_passed += 1

    except Exception as exc:
        print("fav_numbers create table FAILED")
        print(traceback.format_exc())
        tests_failed += 1


    query = Query(fav_numbers)

    nums1 = [5, 6, 7]
    nums2 = [25, 26, 27]
    nums3 = [99, 98, 97]

    query.insert(*nums1)
    query.insert(*nums2)
    query.insert(*nums3)

    try:
        # Try selecting 3 keys from above.
        # Make sure each returns only 1 record and that record is correct.
        for testkey in [5, 25, 99]:
            # Get all columns
            s = get_columns(query.select(testkey, 0, [1, 1, 1]))

            # Make sure result is right length
            # At this point select should return a record (not a list of records)
            # So this is checking how many columns are in that record
            assert len(s) == 3

            # Make sure primary key matches
            assert s[0] == testkey

        print("\nfav_numbers primary key select passed")
        tests_passed += 1

    except Exception as exc:
        print("\nfav_numbers primary key select FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    query.update(5, *(5, 12, 20))

    try:
        # Try selecting record just updated
        s = get_columns(query.select(5, 0, [1, 1, 1]))
        assert s == [5, 12, 20]

        print("\nfav_numbers update + primary key select passed")
        tests_passed += 1


    except Exception as exc:
        print("\nfav_numbers update + primary key select FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    try:
        print("correctness testing says attempting to index content column 1")
        query.table.create_index(1)
        print("correctness testing says attempting to index content column 2")
        query.table.create_index(2)

        # Try selecting record with 26 in column 1 (non-primary key)
        s = get_columns(query.select(26, 1, [1, 1, 1]))
        assert s == [25, 26, 27]

        # Insert a new record with 26 in column 1
        query.insert(100, 26, 32)
        # Try selecting 26 in column 1 again, should get 2 records now
        s = query.select(26, 1, [1, 1, 1])
        assert len(s) == 2, "Expected 2 records, instead found {}".format(len(s))
        for record in s:
            assert record.columns[1] == 26, "Expected 26 in column 1. Record found was {}".format(record)

        print("\nfav_numbers non-primary key select passed")
        tests_passed += 1


    except Exception as exc:
        print("\nfav_numbers non-primary key select FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    # print("\nTable dump:")
    # for r in query.table.record_list:
    #     print(r.columns)

    try:
        s = get_columns(query.select(5, 0, [1, 0, 0]))
        assert s == [5, None, None]
        s = get_columns(query.select(5, 0, [0, 1, 0]))
        assert s == [None, 12, None]
        s = get_columns(query.select(5, 0, [0, 0, 1]))
        assert s == [None, None, 20]

        print("\nfav_numbers select certain columns only passed")
        tests_passed += 1

    except Exception as exc:
        print("\nfav_numbers select certain columns only FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    try:
        sum = query.sum(5, 99, 0)
        assert sum == 129, "Sum all entries in column 0, expected 129, found {}".format(str(sum))

        sum = query.sum(5, 99, 1)
        assert sum == 136, "Sum all entries in column 1, expected 136, found {}".format(str(sum))

        sum = query.sum(5, 6, 1)
        assert sum == 12, "Sum first entry in column 1, expected 12, found {}".format(str(sum))

        sum = query.sum(25, 99, 2)
        assert sum == 124, "Sum last two entries in column 2, expected 124, found {}".format(str(sum))

        print("\nfav_numbers sum passed")
        tests_passed += 1

    except Exception as exc:
        print("\nfav_numbers sum FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    try:
        query.delete(25)
        s = query.select(25, 0, [1, 1, 1])
        assert s == None, "Expected None from attempt to select deleted record"

        print("\nfav_numbers delete + select passed")
        tests_passed += 1

    except Exception as exc:
        print("\nfav_numbers delete + select FAILED")
        print(traceback.format_exc())
        tests_failed += 1


    if tests_failed == 0:
        print("\nAll {} tests passed!!! :)".format(str(tests_passed)))
    else:
        print("\nTests passed:", tests_passed)
        print("Tests failed:", tests_failed)


def correctness_testing_after_close(fav_numbers):
    print("\n====Correctness testing after close:====\n")
    tests_passed = 0
    tests_failed = 0

    try:
        assert fav_numbers._name == "fav_numbers"
        assert fav_numbers._num_columns == 3 + Config.METADATA_COLUMN_COUNT
        assert fav_numbers._key == 0

        print("fav_numbers table attributes after closed passed")
        tests_passed += 1

    except Exception as exc:
        print("fav_numbers table attributes after closed FAILED")
        print(traceback.format_exc())
        tests_failed += 1


    query = Query(fav_numbers)

    try:
        # Try selecting 2 keys from above.
        # Make sure each returns only 1 record and that record is correct.
        # Note 25 was deleted in previous block
        for testkey in [5, 99]:
            # Get all columns
            s = get_columns(query.select(testkey, 0, [1, 1, 1]))

            # Make sure result is right length
            # At this point select should return a record (not a list of records)
            # So this is checking how many columns are in that record
            assert len(s) == 3

            # Make sure primary key matches
            assert s[0] == testkey

        print("\nfav_numbers primary key select after closed passed")
        tests_passed += 1

    except Exception as exc:
        print("\nfav_numbers primary key select after closed FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    try:
        query.delete(99)
        print("\nfav_numbers delete after closed passed")
        tests_passed += 1
    except:
        print("\nfav_numbers delete after closed FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    try:
        query.delete(5)
        print("\nfav_numbers delete updated record after closed passed")
        tests_passed += 1
    except:
        print("\nfav_numbers delete updated record after closed FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    if tests_failed == 0:
        print("\nAll {} tests passed!!! :)".format(str(tests_passed)))
    else:
        print("\nTests passed:", tests_passed)
        print("Tests failed:", tests_failed)


def performance_testing():
    print("\n====Performance testing:====\n")

    # Student Id and 4 grades
    # Grades table has 5 columns, key is first column
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)

    assert grades_table._name == "Grades"
    assert grades_table._num_columns == 5
    assert grades_table._key == 0
    print("Create table good")

    return
    ##################################################

    query = Query(grades_table)
    keys = []

    # Measuring Insert Performance
    insert_time_0 = process_time()
    for i in range(0, 10000):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = process_time()

    print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

    # Measuring update Performance
    update_cols = [
        [randrange(0, 100), None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, 10000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()
    print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 10000):
        query.select(choice(keys), [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 10000, 100):
        result = query.sum(i, 100, randrange(0, 5))
    agg_time_1 = process_time()
    print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, 10000):
        query.delete(906659671 + i)
    delete_time_1 = process_time()
    print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

if __name__ == "__main__":
    # Create favorite numbers table + original correctness tests
    db = Database()
    db.open("~/ECS165")
    fav_numbers = db.create_table('fav_numbers', 3, 0)

    correctness_testing(fav_numbers)

    # Close database
    db.close()

    # Reopen database and check data is still there
    db.open("~/ECS165")
    fav_numbers = db.get_table('fav_numbers')

    correctness_testing_after_close(fav_numbers)

    # performance_testing()

    print("\n\n")
