import os

"""
# Only stores data, no methods
"""
class PhysicalPageLocation:
    def __init__(self, path: str, table: str, __range: int, index_within_file: int):
        self.table = table # for when we drop tables
        self.filename = PhysicalPageLocation.filename_from(path, table, __range)
        self.index_within_file = index_within_file
    
    def __eq__(self, other):
        if not isinstance(other, PhysicalPageLocation):
            return False
        
        return (self.filename == other.filename and self.index_within_file == other.index_within_file)
    
    def __hash__(self):
        return hash((self.filename, self.index_within_file))
    
    @staticmethod
    def filename_from(path: str, table: str, __range: int):
        return os.path.join(path,"{}-{}.bin".format(table, str(__range)))
    
    """
    :param path: str    # Operating System path to the directory where this database's files are
    :param table: str   # Name of table that owns the range you are deleting
    :param __range: int # Index within `table` of the range you wish to delete
    """
    @staticmethod
    def delete_range_file(path: str, table: str, __range: int):
        os.remove(PhysicalPageLocation.filename_from(path, table, __range))
    
    @staticmethod
    def delete_table_files(path: str, table: str, num_ranges_in_table: int):
        for page_range_index in range(num_ranges_in_table):
            PhysicalPageLocation.delete_range_file(path, table, page_range_index)