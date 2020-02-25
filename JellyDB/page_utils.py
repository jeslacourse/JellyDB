from JellyDB.config import Config

# Assumes each page will be the size listed in Config
class PageUtils:
    """
    :param page: bytearray  # the variable in memory to write to
    :param value: int       # a number between 0 and 2**64 - 1 to insert as the next record in this page
    :param index: int       # which index in the page to write to (write permissions will be controlled in logical_page.py)
    """
    @staticmethod
    def write(page: bytearray, value: int, index: int):
        if value < 0 or value > Config.MAX_RECORD_VALUE:
            raise Exception("value" + value + " out of bounds")
    
        value_as_bytes = value.to_bytes(Config.RECORD_SIZE_IN_BYTES, Config.INT_BYTE_ORDER, signed=False)
        
        first_byte_in_page_to_write_to = index*Config.RECORD_SIZE_IN_BYTES
    
        for i in range(Config.RECORD_SIZE_IN_BYTES):
            page[first_byte_in_page_to_write_to + i] = value_as_bytes[i]
    
    """
    # Use this to retrieve a value from a physical page!
    :param page: bytearray  # the variable in memory to read from
    :param n: int   # The index of the record to get
    :returns: int   # The record as an integer
    """
    @staticmethod
    def get_record(page: bytearray, offset_within_page: int) -> int:
        first_byte = offset_within_page*Config.RECORD_SIZE_IN_BYTES
        last_byte = first_byte + Config.RECORD_SIZE_IN_BYTES
        return int.from_bytes(page[first_byte:last_byte], Config.INT_BYTE_ORDER, signed=False)