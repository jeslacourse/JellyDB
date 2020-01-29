# IGNORE FOR NOW
# Unnecessary for Phase 1 - will revisit. This would provide Page objects with
# access to their buffers without letting them know whether the page objects
# were on disk or in memory.
class Bufferpool:
    # this will have an array of open buffers, with a hash table telling you
    # which one is open. For Phase 2 we will need an eviction policy and way
    # to write to disk
    def __init__(self):
        pass
    
    """
    :returns: int   # index of buffer you have been uniquely assigned
    """