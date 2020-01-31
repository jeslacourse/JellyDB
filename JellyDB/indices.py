"""
# Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
#
# With this design, we have one separate index per column we want to index. That will allow
# good calls in table.py, such as self.indexes[3].locate(123), which would mean "find the
# RIDs of any records which have value `123` in column 3."
#
# The Table class will keep instances of Index up to date with `insert` and `delete` calls
# when records are added/deleted or column values are edited.
"""


class Indices:

    """
    # It might be useful to keep track of which column in Table that this class
    """
    def __init__(self):
        self.data = {} # map from column numbers to B-trees.

    def has_index_on(self, column: int) -> bool:
        return column in self.data

    """
    # returns the location of all records with the given value in the given column
    """
    def locate(self, column: int, value: int):
        pass

    """
    # After this call, self.locate(column, value) should return a list containing RID.
    """
    def insert(self, column: int, value: int, RID: int):
        pass
    
    """
    # After this call, self.locate(column, value) should return a list that does not contain RID.
    """
    def delete(self, column: int, value: int, RID: int):
        pass

    """
    # Create index on specific column. Should raise Exception if index already exists.
    """
    def create_index(self, column_number: int):
        pass

    """
    # Drop index of specific column. Should raise Exception if index does not exist.
    """
    def drop_index(self, column_number: int):
        pass
