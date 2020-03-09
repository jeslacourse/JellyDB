from JellyDB.table import Table
import threading
# The Table class performs all query behavior; this is just an envelope. This
# class exists because the Professor's test script expects a class named
# "Query" with these methods.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table: Table):
        self.table: Table = table
        self.lock = threading.Lock()
        #self.committed_update_record_location = []

    """
    # See table.py.
    """
    def delete(self, key: int):
        self.table.delete(key)

    """
    # The * combines multiple arguments to the function into one tuple, columns.
    """
    def insert(self, *columns):
        self.table.insert(columns)

    """
    # See table.py.
    """
    def select(self, key: int, column, query_columns):
        return self.table.select(key, column, query_columns)

    """
    # The * combines all arguments to the function after `key` into one tuple, columns.
    """
    def update(self, key: int, *columns, loc_ = None, commit_ = False, abort = False):
        if abort:
            self.abort_in_table(loc_)
        else:
            if commit_ == False:
                self.lock.acquire()
                u = self.table.pre_update(key, columns)
                if u is not False:
                    #u is location
                    #self.committed_update_record_location.append(u)
                    #print('check line 45',u)
                    self.lock.release()
                    return u
                else:
                    self.lock.release()
                    return False
            #output will be [(key,columns)], list of tuples
            elif commit_ and (loc_ is not None):
                self.lock.acquire()
                #index_of_offsets_going_tobe_committed = self.committed_update_record_location.index(loc)
                self.table.update(key,columns, loc_)
                self.lock.release()
                #self.committed_update_record_location = None
            else:
                print('something went wrong')

    def abort_in_table(self, location_):
        self.table.reset_uRID(location_)
        print('finish abort (in query.py)')
    """
    # See table.py.
    # This function is only called on the primary key. (Note from teaching staff)
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        return self.table.sum(start_range, end_range, aggregate_column_index)

    def increment(self, key, column, loc = None, commit = False, abort = False):
        if abort:
            self.abort_in_table(loc)
        else:
            r = self.select(key, self.table._key, [1] * self.table.num_columns)[0]
            if r is not False:
                record = []
                for i, column_ in enumerate(r.columns):
                    record.append(column_)
                #print(record,'this is a list',type(record))
                #print(column)
                updated_columns = [None] * self.table.num_columns
                updated_columns[column] = record[column] + 1
                #print('u',updated_columns,'r',record)
                if commit == False:
                    u_ = self.update(key, *updated_columns, commit)
                    return u_
                else:
                    #print('commit to update',loc,commit)
                    self.update(key, *updated_columns, loc_ = loc, commit_=True)
                    incremented_result = self.select(key, self.table._key, [1] * self.table.num_columns)[0]
                    return incremented_result
            else:
                return False
