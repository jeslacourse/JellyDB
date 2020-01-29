from JellyDB.config import Config

# We can get data from a page by indexing with pageNameGoesHere.data[indexGoesHere]
# Issue: with this approach, we have to access multiple indices and splice them
# together. This is not great.
# Solution: make a method to get 8 bytes from a page, combining them into an int.

# In this file, we refer to each item contained in a page as a "record", even
# though it technically contains values from a single column of a bunch of records.

"""
# You can read or write integers between 0 and 2^64 - 1 to instances of this class.
"""
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(Config.PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < Config.max_records_per_page

    """
    :param value: int   # a number between 0 and 2^64 - 1 to insert as the next record in this page
    """
    def write(self, value: int):
        if value < 0 or value > Config.MAX_RECORD_VALUE:
            raise Exception("`value` out of bounds")
        elif not self.has_capacity():
            raise Exception("You cannot write to a full page")
        
        value_as_bytes = value.to_bytes(Config.RECORD_SIZE_IN_BYTES, Config.INT_BYTE_ORDER, signed=False)

        first_byte_in_page_to_write_to = self.num_records*Config.RECORD_SIZE_IN_BYTES

        for i in range(Config.RECORD_SIZE_IN_BYTES):
            self.data[first_byte_in_page_to_write_to + i] = value_as_bytes[i]

        self.num_records += 1
    
    """
    # Use this to retrieve a value from a head or tail page!
    :param n: int   # The index of the record to get
    :returns: int   # The record as an integer
    """
    def get_record(self, n):
        pass
