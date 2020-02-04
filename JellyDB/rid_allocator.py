from JellyDB.config import Config
from JellyDB.logical_page import LogicalPage

class RIDAllocator:
    def __init__(self):
        self.nextRIDToAssign = Config.START_RID
        self.nextTailRIDToAssign = Config.START_TAIL_RID
    
    def make_page_range(self, col_count: int) -> list:
        pages = []
        for i in range(Config.NUMBER_OF_BASE_PAGES_IN_PAGE_RANGE):
            pages.append(self.make_base_page(col_count))
        pages.append(self.make_tail_page(col_count))

        return pages

    """
    # Allocates a base page with its own unique RID range
    :returns:   # tuple, (lowest RID allocated, highest RID allocated)
    """
    def make_base_page(self, col_count: int) -> LogicalPage:
        base = self.nextRIDToAssign
        bound = base + Config.MAX_RECORDS_PER_PAGE - 1
        self.nextRIDToAssign = bound + 1
        if self.nextRIDToAssign > self.nextTailRIDToAssign:
            raise Exception("Address space full")
        return LogicalPage(col_count, base, bound)
    
    """
    # Allocates a tail page with its own unique RID range
    :returns:   # tuple, (lowest RID allocated, highest RID allocated)
    """
    def make_tail_page(self, col_count: int) -> LogicalPage:
        bound = self.nextTailRIDToAssign
        base = bound - Config.MAX_RECORDS_PER_PAGE + 1
        self.nextTailRIDToAssign = base - 1
        if self.nextTailRIDToAssign < self.nextRIDToAssign:
            raise Exception("Address space full")
        return LogicalPage(col_count, base, bound)

        
