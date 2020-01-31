from JellyDB.config import Config

class RIDAllocator:
    def __init__(self):
        self.nextRIDToAssign = Config.START_RID
    
    """
    # Allocates enough base RIDs to fill up X pages
    :returns:   # tuple, (lowest RID allocated, highest RID allocated)
    """
    def allocate_base_RIDs(self, num_pages_that_range_should_span: int):
        pass
    
    """
    # Allocates enough tail RIDs to fill up X pages
    :returns:   # tuple, (lowest RID allocated, highest RID allocated)
    """
    def allocate_tail_RIDs(self, num_pages_that_range_should_span: int):
        pass
