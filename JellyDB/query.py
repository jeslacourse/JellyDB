from JellyDB.table import Table

# The Table class performs all query behavior; this is just an envelope. This
# class exists because the Professor's test script expects a class named
# "Query" with these methods.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table: Table):
        self.table: Table = table
        self.committed_update_record_location = []

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
    def update(self, key: int, *columns, loc = None, commit = False, abort = False):
        if abort:
            self.abort_in_table(loc)
        else:
            if commit == False:
                u = self.table.pre_update(key, columns)
                if u is not False:
                    #u is location
                    self.committed_update_record_location.append(u)
                    return u
                else:
                    return False
            #output will be [(key,columns)], list of tuples
            elif commit and (loc is not None):
                    index_of_offsets_going_tobe_committed = self.committed_update_record_location.index(loc)
                    self.table.update(self.committed_update_record_location[index_of_offsets_going_tobe_committed]) #linked list
                    self.committed_update_record_location = None
            else:
                print('something went wrong')

    def abort_in_table(self, location_):
        index_of_offsets_going_tobe_committed = self.committed_update_record_location.index(location_)
        self.table.reset_uRID(self.committed_update_record_location[index_of_offsets_going_tobe_committed])
        del self.committed_update_record_location[index_of_offsets_going_tobe_committed]
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
                for i, column in enumerate(r.columns):
                    record.append(column)
                #print(record,'this is a list',type(record))
                updated_columns = [None] * self.table.num_columns
                updated_columns[column] = record[column] + 1
                if not commit:
                    self.update(key, *updated_columns, commit)
                else:
                    self.update(key, *updated_columns, loc, commit)
                    incremented_result = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
                    return incremented_result
            return False
