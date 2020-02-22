from JellyDB.config import Config
from JellyDB.bufferpool import Bufferpool

# This just stores metadata. The real data access happens in Bufferpool. It's
# like this so the page is ignorant of whether it is in memory or on disk.
#
# In this file, we refer to each item contained in a page as a "record", even
# though it technically contains values from a single column of a bunch of records.

"""
# You can read or write integers between 0 and 2**64 - 1 to instances of this class.
"""
class Page:
    def __init__(self, filename: str, bufferpool: Bufferpool):
        self.physical_page_location = bufferpool.allocate_page_id(filename)
        self.bufferpool = bufferpool
    
    """
    :param value: int       # a number between 0 and 2**64 - 1 to insert as the next record in this page
    :param index: int       # which index in the page to write to (write permissions will be controlled in logical_page.py)
    """
    def write(self, value: int, index: int):
        self.bufferpool.write(self.physical_page_location, value, index)
    
    """
    # Use this to retrieve a value from a physical page!
    :param index_of_record_to_get: int   # The index of the record to get
    :returns: int                        # The record as an integer
    """
    def get_record(self, index_of_record_to_get: int) -> int:
        return self.bufferpool.read(self.physical_page_location, index_of_record_to_get)