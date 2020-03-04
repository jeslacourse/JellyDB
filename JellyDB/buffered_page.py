from JellyDB.config import Config
from JellyDB.physical_page_location import PhysicalPageLocation
import difflib

class BufferedPage:
    def __init__(self, physical_page_location: PhysicalPageLocation):
        if physical_page_location is not None:
            self.set_new_page(physical_page_location)
        else:
            #self.physical_page_location = physical_page_location
            #self.data = None
            #self.transactions_using = 0
            #self.dirty = None
            self.valid = False

    def set_new_page(self, physical_page_location: PhysicalPageLocation):
        self.physical_page_location = physical_page_location
        self.transactions_using = 0
        self.dirty = False
        self.valid = True
        with open(physical_page_location.filename, "rb") as page_file: # TODO confirm `b` is a good mode to use
            byte_offset_of_target_page = Config.PAGE_SIZE*physical_page_location.index_within_file
            page_file.seek(byte_offset_of_target_page, 0)
            self.data = bytearray(page_file.read(Config.PAGE_SIZE))

    def set_new_read_only_page(self, physical_page_location: PhysicalPageLocation):
        self.physical_page_location = physical_page_location
        self.transactions_using = 0
        self.dirty = False
        self.valid = True
        with open(physical_page_location.filename, "rb") as page_file: # TODO confirm `b` is a good mode to use
            byte_offset_of_target_page = Config.PAGE_SIZE*physical_page_location.index_within_file
            page_file.seek(byte_offset_of_target_page, 0)
            self.data = bytearray(page_file.read(Config.PAGE_SIZE))

    """
    # Flushes to disk whether dirty or not
    """
    def flush_to_disk(self):
        if not self.valid:
            raise Exception("Can't flush an invalid BufferedPage")
        if self.transactions_using > 0:
            raise Exception("I can't flush to disk, I am pinned.")
        if self.data is None:
            raise Exception("I have no data to flush to disk!")
        elif self.physical_page_location is None:
            raise Exception("I don't know where to flush my data (if I have any)!")

        with open(self.physical_page_location.filename, "r+b") as page_file: # This mode allows us to override the middle of files
            start_of_page_in_file = self.physical_page_location.index_within_file * Config.PAGE_SIZE
            page_file.seek(start_of_page_in_file, 0)
            page_file.write(self.data)
            self.dirty = False
