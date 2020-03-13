from JellyDB.rid_allocator import RIDAllocator
from JellyDB.indices import Indices
from JellyDB.page import Page
from JellyDB.config import Config
from JellyDB.physical_page_location import PhysicalPageLocation
from JellyDB.xs_lock import XSLock
from time import time_ns
import numpy as np
import threading
from time import process_time
import time
import collections


"""
# the page directory should map from RIDs to this
"""
class RecordLocation:
    def __init__(self, the_range: int, page: int, offset: int):
        self.range = the_range
        self.page = page
        self.offset = offset

class Record:
    def __init__(self, columns):
        self.columns = columns
    def __str__(self):
        return "[{}]".format(", ".join([str(item) for item in self.columns]))

# Why not have a "PageRange" class? Because a "PageRange" is just data with no
# functionality. We can use a list to represent it, and just use functions in
# the Table class to manipulate them.
#
# You must call "open" before you use this class
class Table:

    """
    :param name: str                    # Table name
    :param num_content_columns: int     # Number of content columns
    :param key: int                     # Index of which column has primary key
    """
    def __init__(self, name: str, num_content_columns: int, key: int, RID_allocator: RIDAllocator):
        self._name = name
        self._key = key

        # Number of columns holding actual content
        self._num_content_columns = num_content_columns
        # Same as above (variable name expected by tester.py)
        self.num_columns = num_content_columns
        # Total number of columns, including metadata
        self._num_columns = num_content_columns + Config.METADATA_COLUMN_COUNT

        # Setup boolean array with 1 for primary key column and 0 for all others (used in sum function)
        self._key_column_boolean_array = list(np.zeros((self._num_content_columns,), dtype=int))
        self._key_column_boolean_array[self._key] = 1

        # Allocates RIDs for entire table
        self._RID_allocator = RID_allocator
        # List of values, one for each page range
        self._next_tail_RID_to_allocate = []
        self._page_directory_lock = XSLock()

        # Initialize list of page ranges then create first range
        self._page_ranges = []
        self.record_locks = {}
        self._add_page_range()

        # self._page_directory is a Dictionary of representative rid --> (page range, page no. within range)
        self._recreate_page_directory()

        # Attributes for merging
        self.current_tail_rid = 0
        self.current_base_rid = 0
        self.ranges_with_full_base = []
        self.TPS = [None]
        self.merge_queue = collections.deque()
        self._indices = Indices()
        self._indices.create_index(self.internal_id(self._key))
        self.current_transaction_id = []

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
    Add index to a non-primary key column.
    """
    def create_index(self, column_to_index, verbose=False):
        # Add index to indices class
        self._indices.create_index(self.internal_id(column_to_index))
        # Now populate index

        # Loop through page ranges
        for range_no, page_range in enumerate(self._page_ranges):

            # Loop through base pages in page range
            for logical_base_page in page_range[0:Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE]:

                # Loop through records in base page
                for offset in range(0, Config.MAX_RECORDS_PER_PAGE):
                    record = logical_base_page.read(offset)

                    # Skip records that are empty
                    if record == [0] * self._num_columns:
                        continue

                    current_indirection = record[Config.INDIRECTION_COLUMN_INDEX]

                    # Skip records that are deleted
                    if current_indirection >= Config.RECORD_DELETION_MASK:
                        continue

                    # Base page is already merged, no need to look at tail page
                    if self.TPS[range_no] is not None \
                        and current_indirection > self.TPS[range_no] \
                        and current_indirection <= Config.START_TAIL_RID:
                        record_with_metadata = logical_page_of_target.read(target_loc.offset)

                    # Base page is not merged
                    else:
                        # Page is unmerged but record has not been updated
                        if current_indirection == Config.INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET:
                            latest_version_logical_page = logical_base_page
                            latest_version_offset = offset

                        # Record has been updated, get location of tail record
                        else:
                            tail_record_loc = self.get_record_location(current_indirection)
                            latest_version_logical_page = self._page_ranges[tail_record_loc.range][tail_record_loc.page]
                            latest_version_offset = tail_record_loc.offset

                        record_with_metadata = latest_version_logical_page.read(latest_version_offset)
                        if verbose: print("Here is the latest version of the record:", record_with_metadata)

                    # Insert into index
                    i = self.internal_id(column_to_index)
                    RID = logical_base_page.base_RID + offset
                    self._indices.insert(i, record_with_metadata[i], RID)


    """
    Remove index on a column.
    """
    def drop_index(self, column_to_drop):
        self._indices.drop_index(self.internal_id(column_to_drop))

    """
    # delete the record in self.table which has the value `key` in the column used for its primary key
    :param key: int # the primary key value of the record we are deleting
    """
    def delete(self, key: int, verbose=False):
        if verbose: print("Table delete says: attempting to delete primary key {}".format(key))

        if not self._indices.contains(self.internal_id(self._key), key):
            raise Exception("Primary key {} does not correspond to any record".format(str(key)))

        # Get RID of record to delete
        target_RID = self._indices.locate(self.internal_id(self._key), key)[0]
        target_loc = self.get_record_location(target_RID)

        # Find which logical page record lives on
        target_loc = self.get_record_location(target_RID)
        logical_page_of_target = self._page_ranges[target_loc.range][target_loc.page]

        # Get the most updated logical page
        # This will be a base page if no updates have happened yet
        # Or will be a tail page if it has been updated
        current_indirection = logical_page_of_target.get(Config.INDIRECTION_COLUMN_INDEX, target_loc.offset)
        self.assert_not_deleted(current_indirection)

        # Base page is already merged, no need to look at tail page
        if self.TPS[target_loc.range] is not None \
            and current_indirection > self.TPS[target_loc.range] \
            and current_indirection <= Config.START_TAIL_RID:
            record_with_metadata = logical_page_of_target.read(target_loc.offset)

        # Base page is not merged
        else:
            # Record has not been updated, no need to look at tail page
            if current_indirection == Config.INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET:
                latest_version_logical_page = logical_page_of_target
                latest_version_offset = target_loc.offset

            # Record has been updated, get location of tail record
            else:
                tail_record_loc = self.get_record_location(current_indirection)
                latest_version_logical_page = self._page_ranges[tail_record_loc.range][tail_record_loc.page]
                latest_version_offset = tail_record_loc.offset

            # Get record from most updated logical page
            record_with_metadata = latest_version_logical_page.read(latest_version_offset)

        # bitwise OR
        indirection_value_with_deletion_flag = \
            record_with_metadata[Config.INDIRECTION_COLUMN_INDEX] | Config.RECORD_DELETION_MASK

        # flag as deleted
        self._page_ranges[target_loc.range][target_loc.page] \
            .update_indirection_column(target_loc.offset, indirection_value_with_deletion_flag)

        # delete all values from the index

        if verbose:
            print("Table delete says attempting to delete from index")
            print("Table delete says: record_with_metadata =", record_with_metadata)

            for i in range(self._num_columns):
                if self._indices.has_index(i):
                    print("Table delete says column {} has index".format(i))
                else:
                    print("Table delete says column {} does not have index".format(i))

            print("Table delete says here is self._indices.data.keys()", self._indices.data.keys())

        for i in range(self._num_columns):
            if self._indices.has_index(i):
                self._indices.delete(i, record_with_metadata[i], target_RID)


    """
    # Insert a record with specified columns
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def insert(self, columns: tuple, verbose=False):
        primary_key_value = columns[self._key]
        if self._indices.contains(self.internal_id(self._key), primary_key_value):
            raise Exception("Error: The primary key {} is already in use".format(str(primary_key_value)))

        # Prepend metadata to columns
        # Since this is a new base record, set indirection to 0
        # Base RID metadatacolumn will be 0
        record_with_metadata = [0, time_ns(), 0, 0, *columns]

        # Get next base rid, find what page it belongs to, and write the record to that page
        RID = self._allocate_first_available_base_RID()
        record_location = self.get_record_location(RID)
        self._page_ranges[record_location.range][record_location.page].write(record_with_metadata, record_location.offset)
        #Initialize locks per offset
        self.record_locks[record_location.range][record_location.page].append({record_location.offset:XSLock()})
        # Create entry for this record in index(es)
        for i in range(self.internal_id(0), self.internal_id(self._num_content_columns)):
            if self._indices.has_index(i):
                if verbose: print("table says column {} has index; now inserting in index".format(i))
                self._indices.insert(i, record_with_metadata[i], RID)
            else:
                if verbose: print("table says column {} does not have index; not inserting into index".format(i))

        # Track current base RID
        self.current_base_rid = RID

        # Check if the output is integer
        if (self.current_base_rid % Config.TOTAL_RECORDS_FULL) == 0:
            if verbose: print("Table insert says: Base page full")
            # List of base pages ready to merge
            # Initializing new TPS when there's a new page range
            self.TPS.append(None)
            self.ranges_with_full_base.append([record_location.range,self.current_base_rid])
            #print(self.check_merge)
            if verbose: print("Table insert says: Here is self.ranges_with_full_base:", self.ranges_with_full_base)

        #make sure left-over queues can be processed


    def _allocate_first_available_base_RID(self):
        # Add new page range if necessary
        with self._RID_allocator.lock:
            if not self._page_ranges[-1][Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE - 1].has_capacity():
                self._add_page_range()
            destination_page_range = self._page_ranges[-1]

            for i in range(Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE):
                first_available_RID_in_this_page = destination_page_range[i].first_available_RID()
                if first_available_RID_in_this_page != 0: # page not full
                    destination_page_range[i].record_count += 1 # reserve space for one record in this page.
                    return first_available_RID_in_this_page

            raise Exception("Something went wrong; failed to allocate enough space")



    """
    # Read a record with specified key
    :param keyword: int             # What value to look for
    :param column:                  # Which column to look for that value (default to primary key column)
    :param query_columns: list      # List of integers, one per column. 1 means read the column, 0 means ignore (return None)
    """
    def pre_select(self,keyword, column, query_columns, transaction_id = None, verbose = False):
        RIDs = self._indices.locate(self.internal_id(column), keyword)
        RID = RIDs[0]
        if RIDs is None:
            if verbose: print("Select function says: Indices.py returned None, returning None")
            return False
        if len(RIDs) == 0:
            if verbose: print("Select function says: Indices.py returned empty list, returning None")
            return False
        target_loc = self.get_record_location(RID)
        if transaction_id not in self.current_transaction_id:# no readers read the same record before
            self.current_transaction_id.append(transaction_id)
            #lock dict structure:{[[{}]]}
            if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_S_bool() == False:
                return False
            else:
                #print('I get urid',target_RIDs,threading.current_thread().name,key)
                self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._share_count -= 1
                self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_S()
                return target_loc

        else:#some one already read this record, no need to require lock again
            #should not require the shared lock again
            if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._share_count >0:
                return target_loc
            else:
                if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_S_bool() == False:
                    return False
                else:
                    #print('I get urid',target_RIDs,threading.current_thread().name,key)
                    self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._share_count -= 1
                    self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_S()
                    return target_loc




    def select(self, keyword, column, query_columns, loc, verbose = False):
        if verbose:
            print("Select function says: attempting to locate keyword {} in column {}".format(keyword, column))
            print("Select function says: query_columns = ", query_columns)

        # For columns not asked by user
        not_asked_columns = list(np.where(np.array(query_columns) == 0)[0])

        # Check index on column user requested
        # Get list of base RIDs for records with keyword in that column
        results = []
        target_loc = loc
        # Find what logical page it lives on
        logical_page_of_target = self._page_ranges[target_loc.range][target_loc.page]

        # Get the most updated logical page
        # This will be a base page if no updates have happened yet
        # Or will be a tail page if it has been updated
        current_indirection = logical_page_of_target.get(Config.INDIRECTION_COLUMN_INDEX, target_loc.offset)
        self.assert_not_deleted(current_indirection)

        # Base page is already merged, no need to look at tail page
        if self.TPS[target_loc.range] is not None \
            and current_indirection > self.TPS[target_loc.range] \
            and current_indirection <= Config.START_TAIL_RID:
            record_with_metadata = logical_page_of_target.read(target_loc.offset)

        # Base page is not merged
        else:
            # Record has not been updated, no need to look at tail page
            if current_indirection == Config.INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET:
                latest_version_logical_page = logical_page_of_target
                latest_version_offset = target_loc.offset

            # Record has been updated, get location of tail record
            else:
                tail_record_loc = self.get_record_location(current_indirection)
                latest_version_logical_page = self._page_ranges[tail_record_loc.range][tail_record_loc.page]
                latest_version_offset = tail_record_loc.offset

            # Get record from most updated logical page
            record_with_metadata = latest_version_logical_page.read(latest_version_offset)

        if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._share_count > 2:
            self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].release()
        record = record_with_metadata[self.internal_id(0):]
        if verbose: print("Select function says: here's the record I found:", record)

        if len(not_asked_columns) > 0:
            if verbose:
                print("Select says not asked columns=", not_asked_columns)
                print("Select says record=", record)

            for m in not_asked_columns:
                record[m] = None

        fancy_record = Record(record)
        results.append(fancy_record)

        return results


    def assert_not_deleted(self, value_of_indirection_column: int):
        if value_of_indirection_column >= Config.RECORD_DELETION_MASK:
            raise Exception("You can't update a deleted record")

    """
    # Update a record with specified key and columns
    # "takes as input a list of values for ALL columns of the table. The columns that are not being updated should be passed as None." - Parsoa
    :param key: int   # value in the primary key column of the record we are updating
    :param columns: tuple   # expect a tuple containing the values to put in each column: e.g. (1, 50, 3000, None, 300000)
    """
    def pre_update(self, key:int,columns: tuple, transaction_id = None):
        #uncommitted_record = Node(columns)
        #locate record in current base page
        target_RIDs = self._indices.locate(self.internal_id(self._key), key)
        target_RID = target_RIDs[0]
        target_loc = self.get_record_location(target_RID)
        logical_page_of_target = self._page_ranges[target_loc.range][target_loc.page]
        current_indirection = logical_page_of_target.get(Config.INDIRECTION_COLUMN_INDEX, target_loc.offset)
        self.assert_not_deleted(current_indirection)
        if transaction_id not in self.current_transaction_id:
            if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_X_bool() == False:
                return False
            else:
                self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._exclusive_count -= 1
                self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].acquire_X()
                return target_loc
        else:
            if self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset]._share_count == 1:
                self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].upgrade()
                return target_loc
            else:
                return False

        '''
        current_uRID = logical_page_of_target.get(Config.URID_INDEX, target_loc.offset)
        if current_uRID != 0:
            #print('not get uRID', target_RIDs, threading.current_thread().name, key)
            return False #arbitrary write lock on that record
        else:
            #print('I get urid',target_RIDs,threading.current_thread().name,key)
            logical_page_of_target.update_uRID(target_loc.offset, 1)
            return target_loc
        '''

    def update(self, key,columns:tuple, target_location, verbose=False):
        #if verbose: print("Table says I'm updating at", process_time())
        # Get RID of record to update
        #print("I'm committed update")
        target_loc = target_location
        target_RID = self.get_rid((target_loc.range,target_loc.page), target_loc.offset)
        target_base_RID = target_RID
        #[(key,column),target_loc]
        loc = self.get_record_location(target_RID)
        #target_base_RID = self.get_rid((loc.range,loc.page), loc.offset)

        logical_page_of_target = self._page_ranges[target_loc.range][target_loc.page]

        current_uRID = logical_page_of_target.get(Config.URID_INDEX, target_loc.offset)
        #print(columns)
        #returns [(column tuple)]

        current_indirection = logical_page_of_target.get(Config.INDIRECTION_COLUMN_INDEX, target_loc.offset)
        self.assert_not_deleted(current_indirection)


        # Base page is already merged, no need to look at tail page
        if self.TPS[target_loc.range] is not None \
            and current_indirection > self.TPS[target_loc.range] \
            and current_indirection <= Config.START_TAIL_RID:
            record_with_metadata = logical_page_of_target.read(target_loc.offset)

        # Base page is not merged
        else:
            # Record has not been updated, no need to look at tail page
            if current_indirection == Config.INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET:
                latest_version_logical_page = logical_page_of_target
                latest_version_offset = target_loc.offset

            # Record has been updated, get location of tail record
            else:
                tail_record_loc = self.get_record_location(current_indirection)
                latest_version_logical_page = self._page_ranges[tail_record_loc.range][tail_record_loc.page]
                latest_version_offset = tail_record_loc.offset

            # Get record from most updated logical page
            record_with_metadata = latest_version_logical_page.read(latest_version_offset)

        # Assign this tail record a RID
        tail_RID_of_current_update = self._allocate_next_available_tail_RID(target_loc.range)

        # only time we edit the base page: updating indirection column
        logical_page_of_target.update_indirection_column(target_loc.offset, tail_RID_of_current_update)

        # Build tail record one column at a time
        # Indirection, timestamp, and columns passed
        current_update = []
        for i in range(self._num_columns):
            if i == Config.INDIRECTION_COLUMN_INDEX:
                # old indirection pointer of the base record, which points to the latest update before this one
                current_update.append(current_indirection)
            elif i == Config.TIMESTAMP_COLUMN_INDEX:
                current_update.append(time_ns())
            elif i == Config.BASE_RID_FOR_TAIL_PAGE_INDEX:
                current_update.append(target_base_RID)
            else:
                if columns[self.external_id(i)] is not None:
                    # we have a new value, update it in the index
                    self.replace_if_indexed(i, target_RID, record_with_metadata[i], columns[self.external_id(i)])
                    current_update.append(columns[self.external_id(i)])
                else:
                    current_update.append(record_with_metadata[i])

        current_update_loc = self.get_record_location(tail_RID_of_current_update)

        # Track current tail rid
        self.current_tail_rid = tail_RID_of_current_update
        #self._page_ranges[current_update_loc.range][current_update_loc.page].write(current_update, current_update_loc.offset)
        #preventing main_thread accessing the same resource the _page_directory_reallocation is locking at the same time
        #Important: if KeyError pops out, increase the time.sleep duration!

        self._page_ranges[current_update_loc.range][current_update_loc.page].write(current_update, current_update_loc.offset)
        #release write lock
        print('release update lock')
        self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset].release()
        '''
        logical_page_of_target.update_uRID(target_loc.offset, 0)
        '''
        '''
        if ((Config.START_TAIL_RID-self.current_tail_rid) % Config.MAX_RECORDS_PER_PAGE) == 0:
            if verbose:
                print(self.current_tail_rid)
            #print(current_update_loc.range,'pagerange',len(self._page_ranges))
            self.merge_queue.appendleft([current_update_loc.range, current_update_loc.page, self.current_tail_rid])
            #print('q',len(self.merge_queue))

        if self.merge_queue:
            #not empty
            threads_names = []
            for thread in threading.enumerate():
                threads_names.append(thread.getName())
            if 'merge_thread' not in threads_names:
                for i in range(len(self.merge_queue)):
                    found = False
                    try:
                        #if verbose:
                        print(self.merge_queue[-1],self.merge_queue[-1][0],'sdfasdf')
                        print(self.ranges_with_full_base[self.merge_queue[-1][0]][0])

                        #full base pages and one tail page are both full in the same page range
                        if self.merge_queue[-1][0] == self.ranges_with_full_base[self.merge_queue[-1][0]][0]:
                            found = True
                            break

                    except IndexError:
                        self.merge_queue.appendleft(self.merge_queue.pop())
                        continue
                if found == True:
                    merge_thread = threading.Thread(target = self.merge, args =(self.merge_queue.pop(),), name ='merge_thread')
                    merge_thread.start()
                else:
                    pass

                if verbose:
                    print("i'm still in main thread", threading.current_thread().name)
            else:
                #another thread was running
                pass
        else:
            #empty
            pass
            '''


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
    # written into, creating a new tail page if necessary. Must be atomic!
    """
    def _allocate_next_available_tail_RID(self, target_page_range: int):
        with self._RID_allocator.lock: # no one else allocating any RIDs
            first_available_spot = self._next_tail_RID_to_allocate[target_page_range]
            if first_available_spot == 0:
                self._add_tail_page(target_page_range)
                first_available_spot = self._page_ranges[target_page_range][-1].base_RID
            next_available_spot = first_available_spot + 1
            if next_available_spot > self._page_ranges[target_page_range][-1].bound_RID:
                next_available_spot = 0 # NO SPACE LEFT on current tail page
            self._next_tail_RID_to_allocate[target_page_range] = next_available_spot
            return first_available_spot

    """
    :param start_range: int              # Start of the key range to aggregate
    :param end_range: int                # End of the key range to aggregate
    :param aggregate_column_index: int   # Index of desired column to aggregate
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int, verbose=False):
        # List of values to sum
        summation = []

        # Setup boolean array for which columns to request with select
        # Premade boolean array with 1 for primary key column and 0 for all others
        columns_for_sum = self._key_column_boolean_array.copy()
        # Also set column we want to sum as 1
        columns_for_sum[aggregate_column_index] = 1

        for ids in range(start_range, end_range+1):
            # Find start record first
            try:
                # Select is returning list of record objects
                # Subscript 0 gets first item out
                # And then .columns gets items out of Record object
                # ONLY SUPPORTS SELECTING ON PRIMARY KEY
                summation.append(self.select(ids, self._key, columns_for_sum)[0].columns[aggregate_column_index])
            # Indices.py will throw KeyError if not all RIDs in range had actual records
            # Indices.py will return empty list for values that now have no associated RIDs after update or delete,
            #   resulting in select returning None and a NoneType error
            # Here we skip anytime we catch either of these errors
            except (KeyError, TypeError) as exc:
                if verbose: print("Update says caught {}, skipping".format(type(exc).__name__))
                continue

        return sum(summation)

    # Call merge function
    def merge(self, tail_page_to_work_on, verbose=False):
        print('Im in merge thread')
        _range = tail_page_to_work_on[0]
        _page = tail_page_to_work_on[1]
        _last_tail_rid = tail_page_to_work_on[2]

        base_records_to_replace = []
        replacement_records = {}
        merged_records = []
        if verbose: print(_last_tail_rid)
        #count = 0
        #time.sleep(0.1)

        # Loop backwards through tail records in range
        for rid in range(_last_tail_rid, _last_tail_rid-Config.MAX_RECORDS_PER_PAGE,-1):
            # Get tail record values and corresponding base rid
            while True:
                try:
                    tail_record_location = self.get_record_location(rid)
                    break
                except:
                    print("table.merge (tail section) spinning on page directory lock")
                    raise Exception
                    continue
            current_tail_page = self._page_ranges[tail_record_location.range][tail_record_location.page]
            corresponding_base_rid = current_tail_page.get(Config.BASE_RID_FOR_TAIL_PAGE_INDEX, tail_record_location.offset)

            # Save base rid to be changed to list
            # Add tail record values to replacement records dictionary
            if corresponding_base_rid not in base_records_to_replace:
                base_records_to_replace.append(corresponding_base_rid)
                replacement_records[corresponding_base_rid]=current_tail_page.read(tail_record_location.offset)[self.internal_id(0):]

        if verbose:
            print("i'm in process to merge",threading.current_thread().name)
        # Loop forwards through base records in range
        for baseid in range(self.ranges_with_full_base[_range][-1]-Config.TOTAL_RECORDS_FULL+1, self.ranges_with_full_base[_range][-1]+1):
            # Get current base record values
            while True:
                try:
                    base_record_location = self.get_record_location(baseid)
                    break
                except:
                    print("table.merge (base section) spinning on page directory lock")
                    raise Exception
                    continue
            base_logical_page = self._page_ranges[base_record_location.range][base_record_location.page]
            try:
                 current_base_record = base_logical_page.read(base_record_location.offset)[self.internal_id(0):]
            except KeyError:
                raise KeyError('check line 447, add sleep time')
            # Add replacement values to merged_records dict
            # Or add original values for base records that don't need to be changed
            if baseid in base_records_to_replace:
                merged_records.append(replacement_records[baseid])
            else:
                merged_records.append(current_base_record)
        self._page_directory_reallocation(merged_records,_range,_last_tail_rid)


    def _page_directory_reallocation(self, records, __range,__tail__rid, verbose=False):
        if verbose:
            print("Page directory reallocation says: wait for me to finish",process_time())

        lock = threading.Lock() # TODO i doubt this lock does anything. it is created within a function, so I think any thread calling this function will create its own lock. - Ben
        with lock:
            for n in range(0, Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE):#number of basepage
                self.merge_count = 0
                #base pages will always remain as first several pages in the logical page range
                if verbose: print(len(self._page_ranges[__range][n].pages), threading.current_thread().name)
                self._page_ranges[__range][n].merge_write(self._page_ranges[__range][n].pages,self.num_columns, __range)
                if verbose: print(len(self._page_ranges[__range][n].pages))
                for record in records[n*Config.MAX_RECORDS_PER_PAGE:(n+1)*Config.MAX_RECORDS_PER_PAGE]:
                    for k in range(len(record)):
                        self._page_ranges[__range][n].pages[k+Config.METADATA_COLUMN_COUNT].write(record[k], self.merge_count)
                    self.merge_count+=1
                #reading at logical pages always return first serveral columns, metadata + userdefined columns
                #so merged columns inserted will be directly detected.
            self.TPS[__range] = __tail__rid-Config.MAX_RECORDS_PER_PAGE+1
            while True:
                try:
                    self._recreate_page_directory()
                    break
                except:
                    print("table._page_directory_reallocation spinning on page directory lock")
                    raise Exception
                    continue
        if verbose: print('Table page directory reallocation says: I finished updating, you can go', process_time())
        #print(records)
        #self.lock = None

    def _add_page_range(self):
        with self._RID_allocator.lock:
            self._page_ranges.append(
                self._RID_allocator.make_page_range(self._name, len(self._page_ranges), self._num_columns)
            )
            # keep track of the first tail RID in this new page range
            self._next_tail_RID_to_allocate.append(self._page_ranges[-1][-1].base_RID)
            #create subsets of locks per page range
            #len(self._page_ranges) - 1 will represent the id of corresponding page range
            #when a new page range is created, Initialize a place holder per base page
            self.record_locks[len(self._page_ranges)-1] =[[] for _ in range(Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE)]
            '''
            for basepageid in range(Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE):
                self.record_locks[len(self._page_ranges)-1] = []
            print(basepageid)
            '''
        while True:
            try:
                self._recreate_page_directory()
                break
            except:
                print("table._page_directory_reallocation spinning on page directory lock")
                raise Exception
                continue

    """
    :param page_range: int  # page range to add the tail page to. Only use this if you
    """
    def _add_tail_page(self, page_range: int):
        self._page_ranges[page_range].append(
            self._RID_allocator.make_tail_page(self._name, page_range, self._num_columns)
        )
        while True:
            try:
                self._recreate_page_directory() # TODO does the page directory need to have a lock? I think so
                break
            except:
                print("table._add_tail_page spinning on page directory lock")
                raise Exception
                continue

    """
    # There are 4096 RIDs that might be the base RID for the page this RID is
    # from. We try all of them rather than doing an O(logn) tree search
    #
    # Why so naive? With time limitations, this is nasty but still O(1).
    :param RID: int             # can be a tail or base RID
    :returns: RecordLocation    # position of the record with given RID
    """
    def get_record_location(self, RID: int):
        lowest_RID_that_might_be_the_base_for_this_RIDs_page = \
            RID - (Config.MAX_RECORDS_PER_PAGE - 1)
        with self._page_directory_lock.acquire_S():
            for i in range(lowest_RID_that_might_be_the_base_for_this_RIDs_page, RID + 1):
                page_rng_and_page_index = self._page_directory.get(i)
                if page_rng_and_page_index is not None:
                    page_rng_index = page_rng_and_page_index[0]
                    page_index = page_rng_and_page_index[1]
                    offset = RID - i
                    return RecordLocation(page_rng_index, page_index, offset)

        raise Exception("Record does not exist in this table")

    """
    # This stores tuples of (page_range, page). get_record_location() figures
    # out the offset.
    #
    # TODO improvement: the page directory does not need to be recreated, just appended into
    """
    def _recreate_page_directory(self):
        # if a number is within <Records-in-page> of a record's rids, that's the page for it!
        with self._page_directory_lock.acquire_X():
            self._page_directory = {}
            for i in range(len(self._page_ranges)):
                page_rng = self._page_ranges[i]
                for j in range(len(page_rng)):
                    page = page_rng[j]
                    self._page_directory[page.base_RID] = (i,j)

    def delete_all_files_owned_in(self, path_to_db_files: str):
        PhysicalPageLocation.delete_table_files(path_to_db_files, self._name, len(self._page_ranges))

    def reset_uRID(self,record_location):
        target_loc = record_location
        '''
        logical_page_of_target_to_abort = self._page_ranges[target_loc.range][target_loc.page]
        logical_page_of_target_to_abort.update_uRID(target_loc.offset, 0)
        '''
        #delete all locks
        self.record_locks[target_loc.range][target_loc.page][target_loc.offset][target_loc.offset] = XSLock()

    def get_rid(self,pageidentity:tuple,offset):
        listOfKeys = None
        listOfItems = self._page_directory.items()
        for item in listOfItems:
            #print(item)
            if item[1] == pageidentity:
                listOfKeys = item[0]
        return listOfKeys+offset
