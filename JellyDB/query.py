from JellyDB.table import Table
from JellyDB.index import Index


# How to implement this class: we can, within this class, directly access the members and functions of table.
# We could use those to, say, allocate new pages for insertion. However, that means Query is responsible for
# Table's storage, and you couldn't have multiple query objects on the same table. I don't like that idea.
# The other extreme: get rid of the Query class; Table owns all of the query-like methods.
#
# A "middle-ground" is that Table controls all of its page allocations, so that Query has no idea new pages
# are being allocated for its inserts.
#
# I expect any approach to need revamping when we implement transactions.
#
# Also, it's a mystery where indices fit in - that's something we'll need to Google. They could be part of Query objects,
# but should PROBABLY be members of the table itself so that any Query object can access them.
# If we have indices on non-key attributes, I think that they will be costly to keep up-to-date.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table: Table):
        self.table: Table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """
    def _read(self, rid: int):
        pass

    """
    # delete the record in self.table which has the value `key` in the column used for its primary key
    :param key: int # the primary key value of the record we are deleting
    """
    def delete(self, key: int):
        pass

    """
    # Insert a record with specified columns
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        pass

    """
    # Read a record with specified key
    :param query_columns: list # Expect a list of integers: one per column. There will be a 1 if we are to read the column, a 0 otherwise.
    """
    def select(self, key: int, query_columns):
        pass

    """
    # Update a record with specified key and columns
    # "takes as input a list of values for ALL columns of the table. The columns that are not being updated should be passed as None." - Parsoa
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def update(self, key: int, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        pass
