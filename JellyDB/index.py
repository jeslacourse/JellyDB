from JellyDB.table import Table

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree

# WARNING: DO NOT IMPLEMENT THIS CLASS YET. We are waiting for the TA's reply on how it is supposed to work.
"""


class Index:

    def __init__(self, table):
        pass

    """
    # returns the location of all records with the given value
    """
    def locate(self, value):
        pass

    """
    # optional: Create index on specific column
    """
    def create_index(self, table, column_number: int):
        pass

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, table, column_number: int):
        pass
