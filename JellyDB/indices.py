from JellyDB.xs_lock import XSLock
from JellyDB.intent_xs_lock import IntentXSLock

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
        self.data = {} # map from column numbers to dictionaries.
        self.lock = IntentXSLock()
        self.col_locks = {} # dictionary mapping int to XSLock

    def has_index(self, column: int) -> bool:
        self.lock.acquire_S()
        try:
            return column in self.data
        finally:
            self.lock.release_S()

    """
    # returns list of RIDs, also known as: the location of all records with the given value in the given column
    # Returns "None" if the column exists, but not the value
    """
    def locate(self, column: int, value: int, verbose=False) -> list:
        self.lock.acquire_IS()
        try:
            if verbose: print("Attempting to search for value {} in column {}".format(value, column))
            if column not in self.data:
                raise Exception("No index exists for column {}".format(str(column)))
            with self.col_locks[column].acquire_S():
                return self.data[column][value]
        finally:
            self.lock.release_IS()

    """
    # Checks whether a certain value exists in the given column's index
    """
    def contains(self, column: int, value: int) -> bool:
        self.lock.acquire_IS()
        try:
            if column not in self.data:
                raise Exception("No index exists for column {}".format(str(column)))
            with self.col_locks[column].acquire_S():
                return self.data[column].get(value) is not None
        finally:
            self.lock.release_IS()

    """
    # After this call, self.locate(column, value) should return a list containing RID.
    """
    def insert(self, column: int, value: int, RID: int, verbose=False):
        self.lock.acquire_IX()
        try:
            if column not in self.data:
                raise Exception("No index exists for given column")

            if verbose: print("Attempting to insert keyword {} from RID {} in index on column {}".format(value, RID, column))
            with self.col_locks[column].acquire_X():
                list_of_RIDs_for_this_value = self.data[column].get(value)

                if list_of_RIDs_for_this_value is None:
                    self.data[column][value] = []
                    list_of_RIDs_for_this_value = self.data[column][value]

                list_of_RIDs_for_this_value.append(RID)
        finally:
            self.lock.release_IX()

    """
    # After this call, self.locate(column, value) should return a list that does not contain RID.
    """
    def delete(self, column: int, value: int, RID: int, verbose=False):
        self.lock.acquire_IX()
        try:
            if verbose:
                print("indices delete says here is self.data before delete")
                print(self.data)

            if column not in self.data:
                raise Exception("No index exists for given column")

            with self.col_locks[column].acquire_X():
                # Get RIDs associated with the value given
                list_of_RIDs_for_this_value = self.data[column].get(value)
                if verbose:
                    print("indices delete says here is list of RIDs for value")
                    print(list_of_RIDs_for_this_value)

                # Ensure the RID given is associated with the value given
                if (list_of_RIDs_for_this_value is None) or (RID not in list_of_RIDs_for_this_value):
                    print(list_of_RIDs_for_this_value)
                    raise Exception("Value {} in column {} not associated with rid {}".format(value, column, RID))

                # Remove RID from index on this value
                list_of_RIDs_for_this_value.remove(RID)
                if verbose:
                    print("indices delete says here is self.data after delete:")
                    print(self.data)
        finally:
            self.lock.release_IX()

    """
    # Create index on specific column. Should raise Exception if index already exists.
    """
    def create_index(self, column: int):
        self.lock.acquire_X()
        try:
            if column in self.data and self.data[column] is not None:
                raise Exception("Index already exists")

            self.data[column] = {}
            self.col_locks[column] = XSLock()
        finally:
            self.lock.release_X()

    """
    # Drop index of specific column. Should raise Exception if index does not exist.
    """
    def drop_index(self, column: int):
        self.lock.acquire_X()
        try:
            if column not in self.data:
                raise Exception("No index exists for given column")

            self.data[column] = None
            self.col_locks[column] = None
        finally:
            self.lock.release_X()
