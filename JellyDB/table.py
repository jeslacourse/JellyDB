from JellyDB.rid_allocator import RIDAllocator
from JellyDB.indices import Indices
from JellyDB.logical_page import LogicalPage
from JellyDB.page import Page
from JellyDB.config import Config
from time import time

"""
# the page directory should map from RIDs to this
"""
class RecordLocation:
    def __init__(self, the_range: int, page: int, offset: int):
        self.range = the_range
        self.page = page
        self.offset = offset

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
        self._next_tail_RID_to_allocate = [] # List of values, one for each page range

        self._add_page_range()

        self._recreate_page_directory()
        self._indices.create_index(self.internal_id(key)) # we always want an index on the key

    """
    # The users of our database only know about their data columns. Since we
    # have metadata columns at the beginning of our tables, we must shift any
    # column number they give us to make sure we access the correct column of
    # data.
    """
    def internal_id(self, column: int) -> int:
        return column + Config.METADATA_COLUMN_COUNT

    """
    # You pass in a column as this table sees it, and it spits out a column
    # number that the user would pass to us. Example: we might store
    # (indirection_value, timestamp, student_id). If you pass in 2, which is
    # the column where we store student_id, it will return 0, because this is
    # the 0th data column that the user asked us to create.
    """
    def external_id(self, column: int):
        return column - Config.METADATA_COLUMN_COUNT

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
        record_with_metadata = [0, time(), *columns] # no record for indirection col to point to
        
        RID = self._allocate_first_available_base_RID()
        record_location = self.get_record_location(RID)
        self._page_ranges[record_location.range][record_location.page].write(record_with_metadata)

        for i in len(record_with_metadata):
            if self._indices.has_index(i):
                self._indices.insert(i, record_with_metadata[i], RID)

    def _allocate_first_available_base_RID(self):
        if not self._page_ranges[-1][Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE - 1].hasCapacity():
            self._add_page_range()
        destination_page_range = self._page_ranges[-1]

        for i in range(Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE):
            first_available_RID_in_this_page = destination_page_range[i].first_available_RID()
            if first_available_RID_in_this_page != 0: # page not full
                return first_available_RID_in_this_page

        raise Exception("Something went wrong; failed to allocate enough space")


    """
    # Read a record with specified key
    :param query_columns: list # Expect a list of integers: one per column. There will be a 1 if we are to read the column, a 0 otherwise.
    """
    def select(self, keyword, query_columns):
        # Create empty list of records to return
        results = []

        # Loop through records (brute force)
        #for i in range (0, self.logical_page_list[0].record_count):
        #    current_record = self.logical_page_list[0].read(i)
        #    # If key matches, append record to results
        #    if current_record[0] == keyword:
        #        results.append(current_record)

        # Return list of results (currently all columns)
        # TODO: only return requested columns
        return results
    
    def assert_not_deleted(self, value_of_indirection_column: int):
        if value_of_indirection_column >= Config.RECORD_DELETION_MASK:
            raise Exception("You can't update a deleted record")

    """
    # Update a record with specified key and columns
    # "takes as input a list of values for ALL columns of the table. The columns that are not being updated should be passed as None." - Parsoa
    :param key_index: int   # value in the primary key column of the record we are updating
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def update(self, key_index: int, columns: tuple):
        target_RID = self._indices.locate(self._key, key_index)
        target_loc = self.get_record_location(target_RID)
        logical_page_of_target = self._page_ranges[target_loc.range][target_loc.page]

        # get the latest version of the page
        current_indirection = logical_page_of_target.get(Config.INDIRECTION_COLUMN_INDEX, target_loc.offset)
        self.assert_not_deleted(current_indirection)

        if current_indirection == Config.INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET:
            latest_version_logical_page = logical_page_of_target
            latest_version_offset = target_loc.offset
        else:
            latest_version_loc = self.get_record_location(current_indirection)
            latest_version_offset = latest_version_loc.offset
            latest_version_logical_page = self._page_ranges[latest_version_loc.range][latest_version_loc.page]

        latest_version_of_record = latest_version_logical_page.read(latest_version_offset)
        
        tail_RID_of_current_update = self.allocate_next_available_tail_RID(target_loc.range)

        # only time we edit the base page: updating indirection column
        logical_page_of_target.update_indirection_column(target_loc.offset, tail_RID_of_current_update)

        current_update = []
        for i in range(self._num_columns + Config.METADATA_COLUMN_COUNT):
            if i == Config.INDIRECTION_COLUMN_INDEX:
                # old indirection pointer of the base record, which points to the latest update before this one
                current_update.append(current_indirection)
            elif i == Config.TIMESTAMP_COLUMN_INDEX:
                current_update.append(time())
            else:
                if columns[self.external_id(i)] is not None:
                    # we have a new value, update it in the index
                    self.replace_if_indexed(i, target_RID, latest_version_of_record[i], columns[self.external_id(i)])
                    current_update.append(columns[self.external_id(i)])
                else:
                    current_update.append(latest_version_of_record[i])
        
        current_update_loc = self.get_record_location(tail_RID_of_current_update)

        self._page_ranges[current_update_loc.range][current_update_loc.page][current_update_loc.offset].write(current_update)

    """
    # Convenience method to replace one value in an index with another
    :param internal_col: int    # Column number seen inside the table, which means taking into account metadata columns
    """
    def replace_if_indexed(self, internal_col: int, RID: int, old_value: int, new_value: int):
        if not self._indices.has_index(internal_col):
            return
        self._indices.delete(internal_col, old_value, RID)
        self._indices.insert(internal_col, new_value, RID)


    """
    # Gets the tail RID that a new updated version of a record should be
    # written into, creating a new tail page if necessary
    """
    def allocate_next_available_tail_RID(self, target_page_range: int):
        first_available_spot = self._next_tail_RID_to_allocate[target_page_range]
        if first_available_spot == 0:
            self._add_tail_page(target_page_range)
            first_available_spot = self._page_ranges[target_page_range][-1].base_RID
        next_available_spot = first_available_spot + 1
        if next_available_spot > self._page_ranges[target_page_range][-1].bound_RID:
            next_available_spot = 0 # NO SPACE LEFT
        self._next_tail_RID_to_allocate[target_page_range] = next_available_spot

        return first_available_spot
        


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
        self._page_ranges.append(self._RID_allocator.make_page_range(self._num_columns))
        # keep track of the first tail RID in this new page range
        self._next_tail_RID_to_allocate.append(self._page_ranges[-1][-1].base)
        self._recreate_page_directory()

    """
    :param page_range: int  # page range to add the tail page to
    """
    def _add_tail_page(self, page_range: int):
        self._page_ranges[page_range].append(self._RID_allocator.make_tail_page(self._num_columns))
        self._recreate_page_directory()
    
    """
    :param RID: int             # can be a tail or base RID
    :returns: RecordLocation    # position of the record with given RID
    """
    def get_record_location(self, RID: int):
        base_RID_of_record_where_RID_belongs = RID // Config.MAX_RECORDS_PER_PAGE # floor division
        offset_of_record = RID % Config.MAX_RECORDS_PER_PAGE

        page_rng_index, page_index = self._page_directory[base_RID_of_record_where_RID_belongs]

        return RecordLocation(page_rng_index, page_index, offset_of_record)


    """
    # We exploit the fact that pages have the same number of elements, and that
    # that number is a power of 2; then we only store information about the
    # base RID in any page.
    """
    def _recreate_page_directory(self):
        # if a number is within <Records-in-page> of a record's rids, that's the page for it!
        self._page_directory = {}
        for i in range(len(self._page_ranges)):
            page_rng = self._page_ranges[i]
            for j in range(len(page_rng)):
                page = page_rng[j]
                self._page_directory[page.base_RID] = (i,j)

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
