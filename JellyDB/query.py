from JellyDB.table import Table
from JellyDB.index import Index


# The Table class performs all query behavior; this is just an envelope. This
# class exists because the Professor's test script expects a class named
# "Query" with these methods.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table: Table):
        self.table: Table = table

    """
    # See table.py.
    """
    def delete(self, key: int):
        self.table.delete(key)

    """
    # The * combines multiple arguments to the function into one tuple, columns.
    """
    def insert(self, *columns):
        self.table.insert(columns)

    """
    # See table.py.
    """
    def select(self, key: int, query_columns):
        return self.table.select(key, query_columns)

    """
    # The * combines all arguments to the function after `key` into one tuple, columns.
    """
    def update(self, key: int, *columns):
        self.table.update(key, columns)

    """
    # See table.py.
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        return self.table.sum(start_range, end_range, aggregate_column_index)
