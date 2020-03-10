from JellyDB.config import Config
from JellyDB.bufferpool import Bufferpool
from JellyDB.page import Page

# This logical page has one physical page for every column.
# If a base bage: INDIRECTION, RID, TIMESTAMP, and SCHEMA_ENCODING + all data columns, e.g. USER_ID
# If a tail page: INDIRECTION, RID, and TIMESTAMP + all data columns, e.g. USER_ID
# However, this class will be oblivious to whether a column is data or
# metadata... it just knows it holds several physical pages
class LogicalPage:
    def __init__(self, table: str, __range: int, num_columns: int, base_RID: int, bound_RID: int, bufferpool: Bufferpool):
        self.num_columns = num_columns
        self.base_RID = base_RID
        self.bound_RID = bound_RID
        self.tablename = table
        self.bufferpool_= bufferpool
        self.capacity = self.bound_RID - self.base_RID + 1
        self.record_count = 0

        # Create new array of Page objects, one per col
        self.pages = []
        for i in range(self.num_columns):
            self.pages.append(Page(table, __range, bufferpool))

    """
    # Read one piece of data from one column in this page
    """
    def get(self, column: int, offset: int):
        return self.pages[column].get_record(offset)

    # Lisa added this function
    # Read all columns of a record
    def read(self, id):
        # Create empty list of values
        values = []

        # Read values from all columns
        for i in range(self.num_columns):
            values.append(self.pages[i].get_record(id))

        return values

    """
    # Write a record somewhere in this page (one value per column).
    :param index: int   # the offset to write to within this LogicalPage. Pages being "full" are managed at Table level
    :param record: list # a list containing one value (possibly `None`) for each column in this kind of page.
    """
    def write(self, record: list, index: int, verbose=False):
        if index >= self.capacity:
            print("self.base_RID", self.base_RID, "self.bound_RID", \
            self.bound_RID, "index", str(index), "self.capacity", self.capacity)
            raise Exception("Index out of bounds")

        # Loop through columns in record
        # Write each value to the corresponding page
        if verbose: print(str(self.base_RID) + " " + str(self.bound_RID))
        for i in range(len(record)):
            self.pages[i].write(record[i], index)

    def merge_write(self, pages_, columns, range__):
        '''this line decide whether to discard the old records, if want to keep old records
        delete this line 74'''
        pages_ = pages_[:Config.METADATA_COLUMN_COUNT]
        for i in range(columns): #PAGES FOR MERGED BASE RECORDS, insert into the original Pages() (column store)
            #pages_.insert(i+Config.METADATA_COLUMN_COUNT, Page(self.tablename, range__, self.bufferpool_))
            pages_.append(Page(self.tablename, range__, self.bufferpool_))
    """
    # Indirection column is the only in-place update that happens in L-store.
    """
    def update_indirection_column(self, offset: int, value: int):
        self.pages[Config.INDIRECTION_COLUMN_INDEX].write(value, offset)
    
    """
    :returns: int   # if all RIDs in this page have been given to records, this value is 0. Otherwise, it is the RID of the next available RID in this page.
    """
    def first_available_RID(self) -> int:
        if not self.has_capacity():
            return 0
        return self.base_RID + self.record_count
    
    def has_capacity(self) -> bool:
        return self.record_count < self.capacity 
