from JellyDB.table import Table
import threading
import collections
# The Table class performs all query behavior; this is just an envelope. This
# class exists because the Professor's test script expects a class named
# "Query" with these methods.

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table: Table):
        self.table: Table = table


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
    def select(self, key: int, column,query_columns, transac_id_, loc_, commit=False, abort=False, select_in_same_transac = False):
        if abort:
            #print('abort in selection',abort)
            self.abort_in_table(loc_)
        else:
            #print('commit status in select_select',key, column,transac_id_,commit,loc_)
            if not commit:
                #print('commit status in select',key, column,transac_id_,commit,loc_)

                r_ok = self.table.pre_select(key, column, query_columns, select_in_same_transac_called = select_in_same_transac,transaction_id = transac_id_)
                if r_ok is not False:
                    return r_ok
                else:
                    return False
            elif commit and (loc_ is not None):
                #print('commit to select',transac_id_,loc_,commit)
                return self.table.select(key, column, query_columns, loc_, select_in_same_transac_called = select_in_same_transac)
            else:
                print('something went wrong')

    """
    # The * combines all arguments to the function after `key` into one tuple, columns.
    """
    def update(self, key: int, *columns, transac_id_,loc_, commit_ = False, abort = False):
        if abort:
            self.abort_in_table(loc_)
        else:
            if not commit_:
                u = self.table.pre_update(key, columns,transaction_id = transac_id_)
                if u is not False:
                    #u is location
                    #self.committed_update_record_location.append(u)
                    return u
                else:
                    return False
            elif commit_ and (loc_ is not None):
                #index_of_offsets_going_tobe_committed = self.committed_update_record_location.index(loc)
                self.table.update(key,columns, loc_)
                #self.committed_update_record_location = None
            else:
                print('something went wrong')

    def abort_in_table(self, location_):
        self.table.reset(location_)
        #print('finish abort (in query.py)')
    """
    # See table.py.
    # This function is only called on the primary key. (Note from teaching staff)
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        return self.table.sum(start_range, end_range, aggregate_column_index)

    def increment(self, key, column, transac_id, loc, commit = False, abort=False):
        if abort:
            self.abort_in_table(loc)
        else:
            assert_if_record_can_be_read = self.select(key, self.table._key, [1] * self.table.num_columns, transac_id_ = transac_id, loc_= None, select_in_same_transac = True)
            if assert_if_record_can_be_read != False:
                r = self.select(key, self.table._key, [1] * self.table.num_columns, transac_id_ = transac_id,loc_ = assert_if_record_can_be_read, commit = True, select_in_same_transac = True)[0]
                record = []
                for i, column_ in enumerate(r.columns):
                    record.append(column_)
                #print(record,'this is a list',type(record))
                #print(column)
                updated_columns = [None] * self.table.num_columns
                updated_columns[column] = record[column] + 1
                if not commit:
                    #print('commit status in increment',key, column,transac_id,commit__,loc)
                    u_ = self.update(key, *updated_columns, transac_id_ = transac_id,loc_ = None, commit_ = None, abort = None)
                    return u_
                else:
                    #print('commit to update',loc,commit)
                    self.update(key, *updated_columns,transac_id_ = transac_id, loc_ = loc, commit_=1, abort = None)
                    incremented_result = self.select(key, self.table._key, [1] * self.table.num_columns,transac_id_ = transac_id,loc_ = assert_if_record_can_be_read, commit = 1, abort = None)[0]
                    return incremented_result
            else:
                self.abort_in_table(loc)
                return False
