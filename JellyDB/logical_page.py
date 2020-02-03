from JellyDB.config import Config
from JellyDB.page import Page

# This logical page has one physical page for every column.
# If a base bage: INDIRECTION, RID, TIMESTAMP, and SCHEMA_ENCODING + all data columns, e.g. USER_ID
# If a tail page: INDIRECTION, RID, and TIMESTAMP + all data columns, e.g. USER_ID
# However, this class will be oblivious to whether a column is data or
# metadata... it just knows it holds several physical pages
class LogicalPage:
    def __init__(self, num_columns: int, base_RID: int, bound_RID: int):
        self.record_count = 0
        self.num_columns = num_columns
        self.base_RID = base_RID
        self.bound_RID = bound_RID

        # Create new array of Page objects, one per col
        self.pages = []
        for i in range (0, num_columns):
            self.pages.append(Page())


    def has_capacity(self):
        return self.record_count < Config.MAX_RECORDS_PER_PAGE

    """
    # Read one piece of data from one column in this page
    """
    def get(self, column: int, offset: int):
        # TODO implement
        pass

    # Lisa added this function
    # Read all columns of a record
    def read(self, id):
        # Create empty list of values
        values = []

        # Read values from all columns
        for i in range (0, self.num_columns):
            values.append(self.pages[i].read(id))

        return values

    """
    # Append a record to this page (one value per column).
    :param record: tuple  # a tuple containing one value (possibly `None`) for each column in this kind of page.
    """
    def write(self, record: tuple):
        if not self.has_capacity():
            raise Exception("You cannot write to a full page")

        # Loop through columns in record
        # Write each value to the corresponding page
        for i in range (0, len(record)):
            self.pages[i].write(record[i])

        self.record_count += 1
