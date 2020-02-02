from JellyDB.rid_allocator import RIDAllocator
from JellyDB.indices import Indices
from time import time

INDIRECTION_COLUMN = 0
TIMESTAMP_COLUMN = 1
METADATA_COLUMN_COUNT = 2

class Record:

    def __init__(self, rid, key_index, columns):
        self.rid = rid
        self.key_index = key_index
        self.columns = columns

# Why not have a "PageRange" class? Because a "PageRange" is just data with no
# functionality. We can use a list to represent it, and just use functions in
# the Table class to manipulate them.
class Table:

    """
    :param name: str            #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name: str, num_columns, key: int, RID_allocator: RIDAllocator):
        self._name = name
        self._key = key
        self._num_columns = num_columns
        self._RID_allocator = RID_allocator
        self.record_count = 0
        self._indices = Indices()
        self._page_directory = {} # only one copy of each key can be present in a page directory! i.e. records can't have the same key!
        self._page_ranges = [] # list of lists of pages.
        self._add_page_range()

        self._recreate_page_directory()
        self._indices.create_index(self.internal_id(key)) # we always want an index on the key

        self.record_list = []
        self.next_rid = 0

        pass

    """
    # The users of our database only know about their data columns. Since we
    # have metadata columns at the beginning of our tables, we must shift any
    # column number they give us to make sure we access the correct column of
    # data.
    """
    def internal_id(self, column: int) -> int:
        return METADATA_COLUMN_COUNT + column

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
    def insert(self, columns: tuple):
        next_rid = self.next_rid
        new_record = Record(next_rid, 0, columns)
        self.record_list.append(new_record)
        self.next_rid += 1

    """
    # Read a record with specified key
    :param query_columns: list # Expect a list of integers: one per column. There will be a 1 if we are to read the column, a 0 otherwise.
    """
    def select(self, keyword, query_columns):
        results = []

        # Loop through records (brute force)
        for r in self.record_list:
            # If key matches, append record to results
            if keyword == r.columns[r.key_index]:
                results.append(r)

        # Return list of results (currently all columns)
        # TODO: only return requested columns
        return results

    """
    # Update a record with specified key and columns
    # "takes as input a list of values for ALL columns of the table. The columns that are not being updated should be passed as None." - Parsoa
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def update(self, key: int, columns: tuple):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        pass

    def __merge(self):
        pass

    def _add_page_range(self):
        # TODO request base and tail RIDs from self._RID_allocator and generate storage for all base pages and one tail page
        pass

    """
    :param page_range: int  # page range to add the tail page to
    """
    def _add_tail_page(self, page_range: list):
        # TODO get tail RIDs, generate the storage, add it to self.page_ranges[page_range]
        self._recreate_page_directory()

    def _recreate_page_directory(self):
        # search through the pages in the page directory, check the base and
        # bound RIDs in each LogicalPage, and put them in the page directories.
        # example: RID 1111101 corresponds to the tenth row of the first tail
        # page of the second PageRange. So, we need 2 indices and an offset:
        # data[1][Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE][9]. Recall that
        # the first index of any Python list is 0.
        # So, our page directory can be a dictionary with key type int (for
        # our RID) and value type tuple-with-three-elements; in this case
        # (1, Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE, 9).
        # Why this Config number? Because there are a fixed number of base
        # pages in any page range; anything after that must be a tail page!
        pass
