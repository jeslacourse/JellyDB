class Config:
    # PUT ALL CONSTANTS HERE
    _HARD_DISK_PAGE_SIZE = 4096
    RECORD_SIZE_IN_BYTES = 8
    MAX_RECORD_VALUE = 2^(RECORD_SIZE_IN_BYTES*8) - 1
    # Every page of database data allocated will have this many bytes in it
    PAGE_SIZE = _HARD_DISK_PAGE_SIZE
    MAX_RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE_IN_BYTES
    # Every database starts counting RIDs at 1.
    START_RID = 1
    # Store the biggest bit of any data first
    INT_BYTE_ORDER = 'big'
    NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE = 16
