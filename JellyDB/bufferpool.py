from JellyDB.page_utils import PageUtils
from JellyDB.config import Config
from JellyDB.physical_page_location import PhysicalPageLocation
from JellyDB.buffered_page import BufferedPage
import threading
import os

# This provides Page objects with access to their buffers without letting them
# know whether the page objects were on disk or in memory. 
# We are lazy - only increase size of bufferpool when someone requests something.
#
# You must call "open" before using this class.
class Bufferpool:
    def __init__(self):
        pass 

    def _allocate_members(self):
        self.data = []
        # map from PhysicalPageLocation to page's location within `self.data`
        self.where_to_find_page_in_pool = {}
        self.lru_tracker = []
        self.lock = threading.RLock() # Using RLock allows us to have one function that needs the lock call another that needs the lock
    
    def _deallocate_members(self):
        self.data = None
        self.where_to_find_page_in_pool = None
        self.lru_tracker = None
        self.lock = None

    """
    # Looks at how big the file is (i.e. how many pages have been stored there
    # already) and returns the number of pages already present. That number will
    # index into the beginning of this page's space in the file. This method
    # creates files if they don't exist; so NOWHERE ELSE will will we have to
    # create files.

    :returns: PhysicalPageLocation   # the unique disk location of YOUR (you being a physical page) data
    """
    def allocate_page_id(self, table: str, __range: int) -> PhysicalPageLocation:
        page = open(PhysicalPageLocation.filename_from(self.path_to_db_files, table, __range), 'a+b')
        number_of_pages_already_in_file = page.tell() // Config.PAGE_SIZE
        page.write(b"\x00" * Config.PAGE_SIZE) # Guarantees there is enough space to store on disk BEFORE we start performing transactions
        page.close()
        return PhysicalPageLocation(self.path_to_db_files, table, __range, number_of_pages_already_in_file)
    
    def pin(self, page: BufferedPage):
        page.transactions_using += 1
    
    def unpin(self, physical_page_location: PhysicalPageLocation): # a PhysicalPageLocation will be given upon a call to allocate_page_id()
        with self.lock:
            target = self._get_page(physical_page_location, False)
            if (target.transactions_using <= 0):
                self.lock.release()
                raise Exception("The page {} has been unpinned more times than it has been pinned.".format(physical_page_location))
            target.transactions_using -= 1

    def write(self, physical_page_location: PhysicalPageLocation, value: int, index: int):
        with self.lock: # Make sure page is ABSOLUTELY in the bufferpool; pin it so it can't leave
            buffered_page = self._get_page(physical_page_location, True)
            self.pin(buffered_page)
        PageUtils.write(buffered_page.data, value, index)
        buffered_page.dirty = True
        self.unpin(physical_page_location)
    
    def read(self, physical_page_location: PhysicalPageLocation, offset_within_page: int) -> int:
        with self.lock: # make sure that the page is ABSOLUTELY in the bufferpool; pin it so it can't leave
            buffered_page = self._get_page(physical_page_location, True)
            self.pin(buffered_page)
        # now that it is pinned, it won't leave
        val = PageUtils.get_record(buffered_page.data, offset_within_page)
        self.unpin(physical_page_location)
        return val
    
    def _get_frame_number_for_page(self, physical_page_location: PhysicalPageLocation) -> int: # must be atomic because an encapsulating function (_get_page) must be atomic
        if physical_page_location not in self.where_to_find_page_in_pool:
            return self._load_into_memory(physical_page_location)
        return self.where_to_find_page_in_pool[physical_page_location]
    
    """
    # Updates the frames' LRU
    """
    def _get_page(self, physical_page_location: PhysicalPageLocation, update_LRU: bool) -> BufferedPage: # must be atomic because the loading & pinning processe is atomic
        frame = self._get_frame_number_for_page(physical_page_location)
        if update_LRU:
            if frame in self.lru_tracker: # This "if" allows for the case when a frame has just been created and is not in the list
                self.lru_tracker.remove(frame)
            self.lru_tracker.append(frame)
        return self.data[frame]

    """
    :returns:   # Frame number where the requested page has been loaded
    """
    def _load_into_memory(self, physical_page_location: PhysicalPageLocation) -> int: # must be atomic
        frame_where_new_page_belongs = self._find_a_free_frame()
        page = self.data[frame_where_new_page_belongs]
        page.set_new_page(physical_page_location)
        self.where_to_find_page_in_pool[physical_page_location] = frame_where_new_page_belongs
        return frame_where_new_page_belongs
    
    def _find_a_free_frame(self) -> int: # must be atomic - the index returned must be accurate
        if len(self.data) >= Config.BUFFERPOOL_SIZE_IN_PAGES:
            return self._evict_least_recently_used_page()
        else:
            index_of_new_frame = len(self.data)
            self.data.append(BufferedPage(None))
            return index_of_new_frame
            

    """
    # Only call if bufferpool size is > 0.
    """
    def _evict_least_recently_used_page(self) -> int: # must be atomic
        frame_of_page_to_evict = self._get_index_of_LRU_page_we_can_evict()
        page_to_evict = self.data[frame_of_page_to_evict]
        if page_to_evict.valid and page_to_evict.dirty:
            page_to_evict.flush_to_disk()
        del self.where_to_find_page_in_pool[page_to_evict.physical_page_location] # This page will no longer be able to be found in the index
        page_to_evict.valid = False
        
        return frame_of_page_to_evict
    
    def _get_index_of_LRU_page_we_can_evict(self): # must be atomic
        for i in self.lru_tracker:
            if (self.data[i]).transactions_using == 0:
                return i
        raise Exception("All frames are pinned")
    
    def _flush_all_data_to_disk(self): # must be atomic
        for buffered_page in self.data:
            if buffered_page.valid and buffered_page.dirty:
                buffered_page.flush_to_disk()

    def close(self):
        self._flush_all_data_to_disk()
        self._deallocate_members()


    # When db.open
    def open(self, path: str):
        self._allocate_members()
        self.path_to_db_files = path


    def invalidate_pages_of(self, table: str):
        self.lock.acquire()

        for page in self.data:
            if page.valid and page.physical_page_location.table == table:
                if page.transactions_using > 0:
                    raise Exception(
                        "cannot invalidate page {} in bufferpool; {} transactions are using it".format(str(page), str(page.transactions_using))
                    )
                page.valid = False

        self.lock.release()
