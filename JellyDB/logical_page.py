from config import Config

# This logical page has one physical page for every column.
# If a base bage: INDIRECTION, RID, TIMESTAMP, and SCHEMA_ENCODING + all data columns, e.g. USER_ID
# If a tail page: INDIRECTION, RID, and TIMESTAMP + all data columns, e.g. USER_ID
# However, this class will be oblivious to whether a column is data or
# metadata... it just knows it holds several physical pages
class LogicalPage:
    def __init__(self, num_columns: int, base_RID: int, bound_RID: int):
        self.num_records = 0
        self.num_columns = num_columns
        self.base_RID = base_RID
        self.bound_RID = bound_RID
        # TODO: create new array of Page objects, one per col
        pass

    def has_capacity(self):
        return self.num_records < Config.max_records_per_page
    
    """
    # Read one piece of data from one column in this page
    """
    def get(self, column: int, offset: int):
        # TODO implement
        pass
    
    """
    # Append a record to this page (one value per column).
    :param record: tuple  # a tuple containing one value (possibly `None`) for each column in this kind of page.
    """
    def write(self, *record):
        if not self.has_capacity():
            raise Exception("You cannot write to a full page")
        # TODO write one item from `record` to the end of each column

        self.num_records += 1
        pass