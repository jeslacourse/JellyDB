from JellyDB.rid_allocator import RIDAllocator
from JellyDB.bufferpool import Bufferpool
from JellyDB.table import Table
import pickle
import os

class DBDataBundle():
    def __init__(self, tables: dict, bufferpool, RID_allocator):
        self.tables = tables
        self.bufferpool = bufferpool
        self.RID_allocator = RID_allocator

class Database():
    DATABASE_FILE_NAME = "db.bin"
    def __init__(self):
        pass

    def open(self):
        if os.path.exists(Database.DATABASE_FILE_NAME):
            with open(Database.DATABASE_FILE_NAME, "r") as db_backup:
                db_data_bundle: DBDataBundle = pickle.load(db_backup)
                self.tables = db_data_bundle.tables
                self.bufferpool = db_data_bundle.bufferpool
                self.RID_allocator = db_data_bundle.RID_allocator

                for table in self.tables:
                    table.open()
                self.bufferpool.open()
        else:
            # if this is the first time starting up:
            self.tables = {}
            self.bufferpool = Bufferpool()
            self.RID_allocator = RIDAllocator(self.bufferpool)

    def close(self):
        self.bufferpool.close()
        for table in self.tables:
            table.close()

        to_pickle = DBDataBundle(self.tables, self.bufferpool, self.RID_allocator)
        with open(Database.DATABASE_FILE_NAME, "w+") as db_file:        
            pickle.dump(to_pickle, db_file)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key: int) -> Table:
        if name in self.tables:
            raise Exception("Table `{}` already exists".format(name))
        self.tables[name] = Table(name, num_columns, key, self.RID_allocator)
        return self.tables[name]

    """
    # Deletes the specified table, including its pages
    """
    def drop_table(self, name: str):
        if name not in self.tables:
            raise Exception("Table `{}` does not exist".format(name))
        # TODO delete pages
        self.tables[name] = None
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name: str):
        if name not in self.tables:
            raise Exception("Table `{}` does not exist".format(name))
        return self.tables[name]
