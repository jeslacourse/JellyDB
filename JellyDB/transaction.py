from JellyDB.table import Table, Record
import inspect
from JellyDB.query import Query
#from JellyDB.index import Index
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.committed_update_record_location = []
        self.committed_select_record_location = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        #print(self.queries)
        #print(len(self.queries), threading.current_thread().name)
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            # writing queries returns record location
            #print(self.committed_update_record_location, threading.current_thread().name)
            if result == False:
                return self.abort(query, args)
            else:
                if query.__name__ == 'increment' or 'update':
                    #print('in transaction line 39',result)
                    self.committed_update_record_location.append(result)
                if query.__name__ == 'select':
                    self.committed_select_record_location.append(result)
        #print('finished pre_check')
        return self.commit()

    def abort(self,query,args):
        if (self.committed_select_record_location) != [] and (query.__name__ == 'select'):
            for items in self.committed_select_record_location:
                query(*args,loc = items, abort = True)
            self.committed_select_record_location.clear()
        if (self.committed_update_record_location != []) and (query.__name__ == 'increment' or 'update'):
            for items in self.committed_update_record_location:
                query(*args,loc = items, abort = True)
            self.committed_update_record_location.clear()
        return False

    def commit(self):
        #index_for_query = self.queries.index((query, args))
        #print(index_for_query)
        #print(query.__name__, type(query.__name__))
        count = 0
        for query, args in self.queries:
            #print(query.__name__)
            if query.__name__ == 'increment'or'update':
                query(*args, loc = self.committed_update_record_location[count], commit = True)
            if query.__name__ == 'select':
                read_output = query(*args, loc = self.committed_select_record_location[count], commit = True)
                count+=1
        return True

        #del self.queries[index_for_query]
