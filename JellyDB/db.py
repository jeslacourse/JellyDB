from JellyDB.rid_allocator import RIDAllocator
from JellyDB.physical_page_location import PhysicalPageLocation
from JellyDB.bufferpool import Bufferpool
from JellyDB.table import Table
import pickle
import os

class DBDataBundle():
    def __init__(self, tables: dict, bufferpool, RID_allocator, indices):
        self.tables = tables
        self.bufferpool = bufferpool
        self.RID_allocator = RID_allocator
        self.indices = indices

class Database():
    DATABASE_FILE_NAME = "db.bin"
    def __init__(self):
        pass

    def open(self, path_to_db_files: str):
        # Get filename of backup
        self.path_to_db_files = os.path.expanduser(path_to_db_files)
        self.db_backup_filename = os.path.join(self.path_to_db_files, Database.DATABASE_FILE_NAME)

        # Load pickled data
        if os.path.exists(self.db_backup_filename):
            with open(self.db_backup_filename, "rb") as db_backup:
                db_data_bundle = pickle.load(db_backup)
                self.tables = db_data_bundle.tables

                # Reload indices
                for table in self.tables.values():
                    table.reload_ephemeral_structures(db_data_bundle.indices[table._name])

                self.bufferpool = db_data_bundle.bufferpool
                self.RID_allocator = db_data_bundle.RID_allocator

        else:
            # if this is the first time starting up:
            self.tables = {}
            self.bufferpool = Bufferpool()
            self.RID_allocator = RIDAllocator(self.bufferpool)

        self.bufferpool.open(self.path_to_db_files)

    def close(self, verbose=False):
        self.bufferpool.close()

        # Dictionary of indices objects to pickle
        # Key: table name; value: indices object
        indices_to_pickle = {}

        for table in self.tables:
            if verbose: print("Close says: dealing with table {}".format(self.tables[table]._name))
            # Copy each table's indices
            indices_to_pickle[self.tables[table]._name] = self.tables[table]._indices
            # Deallocate indices
            self.tables[table].deallocate_ephemeral_structures()

        # Pickle data bundle
        with open(self.db_backup_filename, "w+b") as db_file:
            to_pickle = DBDataBundle(self.tables, self.bufferpool, self.RID_allocator, indices_to_pickle)
            pickle.dump(to_pickle, db_file)
            self.path_to_db_files = None
            self.db_backup_filename = None

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
    def drop_table(self, table: str):
        if table not in self.tables:
            raise Exception("Table `{}` does not exist".format(table))
        self.bufferpool.invalidate_pages_of(table)
        self.tables[table].drop(self.path_to_db_files)
        self.tables[table].delete_all_files_owned_in(self.path_to_db_files)
        self.tables[table] = None

    """
    # Returns table with the passed name
    """
    def get_table(self, name: str):
        if name not in self.tables:
            raise Exception("Table `{}` does not exist".format(name))
        return self.tables[name]
