from JellyDB.config import Config
from JellyDB.bufferpool import Bufferpool

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

    # Naive implementation of page as list of values
    def __init__(self):
        self.value_list = []

    def read(self, offset):
        return self.value_list[offset]

    def write(self, value):
        self.value_list.append(value)

    # def __init__(self, bufferpool: Bufferpool):
    #     self.data = bytearray(Config.PAGE_SIZE)
    #
    # """
    # :param value: int   # a number between 0 and 2^64 - 1 to insert as the next record in this page
    # :param index: int   # which index in the page to write to (write permissions will be controlled in logical_page.py)
    # """
    # def write(self, value: int, index: int):
    #     if value < 0 or value > Config.MAX_RECORD_VALUE:
    #         raise Exception("value" + value + " out of bounds")
    #     #elif not self.has_capacity():
    #     #    raise Exception("You cannot write to a full page")
    #
    #     value_as_bytes = value.to_bytes(Config.RECORD_SIZE_IN_BYTES, Config.INT_BYTE_ORDER, signed=False)
    #
    #     first_byte_in_page_to_write_to = index*Config.RECORD_SIZE_IN_BYTES
    #
    #     for i in range(Config.RECORD_SIZE_IN_BYTES):
    #         self.data[first_byte_in_page_to_write_to + i] = value_as_bytes[i]
    #
    # """
    # # Use this to retrieve a value from a physical page!
    # :param n: int   # The index of the record to get
    # :returns: int   # The record as an integer
    # """
    # def get_record(self, n: int):
    #     pass
