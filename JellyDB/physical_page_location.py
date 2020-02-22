"""
# Only stores data, no methods
"""
class PhysicalPageLocation:
    def __init__(self, table: str, __range: int, index_within_file: int):
        self.table = table # for when we drop tables
        self.filename = PhysicalPageLocation.filename_from(table, __range)
        self.index_within_file = index_within_file
    
    def __eq__(self, other):
        if not isinstance(other, PhysicalPageLocation):
            return False
        
        return (self.filename == other.filename and self.index_within_file == other.index_within_file)
    
    def __hash__(self):
        return hash((self.filename, self.index_within_file))
    
    @staticmethod
    def filename_from(table: str, __range: int):
        return "{}-{}.bin".format(table, str(__range))