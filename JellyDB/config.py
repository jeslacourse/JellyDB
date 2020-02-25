class Config:
    # PUT ALL CONSTANTS HERE
    _HARD_DISK_PAGE_SIZE = 4096
    RECORD_SIZE_IN_BYTES = 8
    MAX_RECORD_VALUE = 2**(RECORD_SIZE_IN_BYTES*8) - 1
    
    # Every page of database data allocated will have this many bytes in it
    PAGE_SIZE = _HARD_DISK_PAGE_SIZE
    MAX_RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE_IN_BYTES
    
    # Every deleted record will have the first available bit flipped 
    RECORD_DELETION_MASK = 2**63
        
    # Every database starts counting RIDs at 1.
    START_RID = 1
    # Every database update RIDs start one below the deletion mask
    START_TAIL_RID = RECORD_DELETION_MASK-1
    
    # Store the biggest bit of any data first
    INT_BYTE_ORDER = 'big'
    # 1 is 512 records per page
    NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE = 2

    # When page range is full and ready for merging
    TOTAL_RECORDS_FULL = MAX_RECORDS_PER_PAGE * NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE

    INDIRECTION_COLUMN_VALUE_WHICH_MEANS_RECORD_HAS_NO_UPDATES_YET = 0

    INDIRECTION_COLUMN_INDEX = 0
    TIMESTAMP_COLUMN_INDEX = 1
    BASE_RID_FOR_TAIL_PAGE_INDEX = 2
    METADATA_COLUMN_COUNT = 3
