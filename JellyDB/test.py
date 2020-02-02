"""
Usage: python -m JellyDB.test
"""
from JellyDB.db import Database
from JellyDB.query import Query
from time import process_time
from random import choice, randrange
import traceback

def correctness_testing():
    print("\n====Correctness testing:====\n")
    tests_passed = 0
    tests_failed = 0

    # Sonic Heroes characters
    db = Database()
    heroes_table = db.create_table('Heroes', 3, 0)

    try:
        assert heroes_table._name == "Heroes"
        assert heroes_table._num_columns == 3
        assert heroes_table._key == 0

        print("Heroes create table passed")
        tests_passed += 1

    except:
        print("Heroes create table FAILED")
        tests_failed += 1

    query = Query(heroes_table)
    keys = []

    query.insert("Sonic", "Sonic", "Speed")
    query.insert("Tails", "Sonic", "Flight")
    query.insert("Knuckles", "Sonic", "Power")

    try:
        for testkey in ["Sonic", "Tails", "Knuckles"]:
            # print("Testing", testkey)
            s = query.select(testkey, [1, 1, 1])
            assert len(s) == 1
            for r in s:
                assert r.columns[r.key_index] == testkey

        print("\nTeam Sonic select passed")
        tests_passed += 1

    except Exception as exc:
        print("\nTeam Sonic select FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    query.insert("Shadow", "Dark", "Speed")
    query.insert("Rouge", "Dark", "Flight")
    query.insert("Omega", "Dark", "Power")

    try:
        for testkey in ["Shadow", "Rouge", "Omega"]:
            # print("Testing", testkey)
            s = query.select(testkey, [])
            assert len(s) == 1
            for r in s:
                assert r.columns[r.key_index] == testkey

        print("\nTeam Dark select passed")
        tests_passed += 1

    except Exception as exc:
        print("\nTeam Dark select FAILED")
        print(traceback.format_exc())
        tests_failed += 1

    query.update("Sonic", *["Sonic", "Sonic", "Napping"])

    # Should get KeyError when trying to update non-existent record
    try:
        query.update("Eggman", *["Eggman", "Villain", "Jerkassery"])
        assert False, "Expected KeyError for Eggman"
    except KeyError:
        print("\nEggman update KeyError passed")
        tests_passed += 1
    except Exception as exc:
        print("\nEggman update KeyError FAILED")
        print(traceback.format_exc())
        tests_failed += 1


    # print("\nTable dump:")
    # for r in query.table.record_list:
    #     print(r.columns)

    if tests_failed == 0:
        print("All tests passed!!! :)")
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
    correctness_testing()
    performance_testing()
    print("\n\n")
