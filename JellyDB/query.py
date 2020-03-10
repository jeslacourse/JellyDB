from JellyDB.table import Table

# The Table class performs all query behavior; this is just an envelope. This
# class exists because the Professor's test script expects a class named
# "Query" with these methods.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    # Queries that succeed should return the result or True
    # Queries that fail must return False
    # Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table):
        self.table: Table = table

    """
    # See also table.py.
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key: int):
        return self.table.delete(key)

    """
    # See also table.py.
    # Insert a record with specified columns
    # The * combines multiple arguments to the function into one tuple, columns.
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        return self.table.insert(columns)

    """
    # See also table.py.
    # Read a record with specified key
    # Returns a list of Record objects upon success
    # Returns False if record locked by 2PL
    # Assume that select will never be called on a key that doesn't exist

    :param key: the key value to select records based on
    :param query_columns: what columns to return. array of 1 or 0 values.
    """
    def select(self, key: int, column, query_columns):
        return self.table.select(key, column, query_columns)

    """
    # The * combines all arguments to the function after `key` into one tuple, columns.
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key: int, *columns):
        return self.table.update(key, columns)

    """
    # See table.py.
    # This function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range

    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        return self.table.sum(start_range, end_range, aggregate_column_index)

    """
    # Increments one column of the record.
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.

    :param key: the primary of key of the record to increment
    :param column: the column to increment
    """
    def increment(self, key, column):
        r = self.select(key, self.table._key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
