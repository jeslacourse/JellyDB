# references used: https://python-3-patterns-idioms-test.readthedocs.io/en/latest/Singleton.html https://www.tutorialspoint.com/python_design_patterns/python_design_patterns_singleton.htm
class Config:
    class __Config:
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

    instance = None

    def __init__(self):
        if Config.instance:
            raise Exception("You are not allowed to directly instantiate this Singleton class. Access a property instead.")
    def __getattr__(self, name):
        if not Config.instance:
            Config.instance = Config.__Config()
        return getattr(self.instance, name)
