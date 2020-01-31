from JellyDB.rid_allocator import RIDAllocator
from JellyDB.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

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
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self._RID_allocator = RID_allocator
        self.page_directory = {} # only one copy of each key can be present in a page directory! i.e. records can't have the same key!
        self.page_ranges = [] # list of lists of pages.
        self.add_page_range()

        self.recreate_page_directory()
        pass

    def __merge(self):
        pass
    
    def add_page_range(self):
        # TODO request base and tail RIDs from self.db and generate storage for all base pages and one tail page
        pass

    """
    :param page_range: int  # index of page range to add the tail page to
    """
    def add_tail_page(self, page_range: int):
        # TODO get tail RIDs, generate the storage, add it to self.page_ranges[page_range]
        self.recreate_page_directory()
    
    def recreate_page_directory(self):
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
