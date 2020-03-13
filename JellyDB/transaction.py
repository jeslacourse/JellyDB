from JellyDB.table import Table, Record
import inspect
from JellyDB.query import Query
#from JellyDB.index import Index
import threading
import random

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = {}
        self.committed_update_record_location = {}
        self.committed_select_record_location = {}
        random_number = random.randint(0,16777215)
        hex_number = format(random_number,'x')
        #intialize random id for each transaction
        self.transac_id = '#'+hex_number
    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        if self.transac_id not in self.queries.keys():
            self.queries[self.transac_id] = [(query, args)]
        else:
            self.queries[self.transac_id].append((query, args))
        #print(self.queries)
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        #print(self.queries)
        #print(len(self.queries), threading.current_thread().name)
        for key_,value in self.queries.items():
            for tuple in value:
                query = tuple[0]
                args_ = tuple[1]
                if query.__name__ == 'select':
                    result = query(*args_, transac_id_ = key_,loc_ = None, commit=None, abort=None)
                else:
                    result = query(*args_, transac_id = key_,loc = None, commit__=None, abort=None)
                # If the query has failed the transaction should abort
                # writing queries returns record location
                #print(self.committed_update_record_location, threading.current_thread().name)
                if result == False:
                    return self.abort(query, args)
                else:
                    if query.__name__ == 'select':
                        if self.transac_id not in self.committed_select_record_location.keys():
                            self.committed_select_record_location[self.transac_id] = [result]
                        else:
                            self.committed_select_record_location[self.transac_id].append(result)
                    else:
                        #print('in transaction line 39',result)
                        if self.transac_id not in self.committed_update_record_location.keys():
                            self.committed_update_record_location[self.transac_id] = [result]
                        else:
                            self.committed_update_record_location[self.transac_id].append(result)
            return self.commit()


        #print('finished pre_check')

    def abort(self,query,args):
        if query.__name__ == 'select':
            if (self.committed_select_record_location) != []:
                for items in self.committed_select_record_location:
                    query(*args,transac_id_ = None, loc_ = items, commit = None,abort = 1)
                del self.committed_select_record_location[self.transac_id]
                del self.queries[self.transac_id]
                return False
            else:
                return False
        else:
            if self.committed_update_record_location != []:
                for items in self.committed_update_record_location:
                    query(*args,transac_id = None,loc = items, commit__ = None, abort = 1)
                del self.committed_update_record_location[self.transac_id]
                del self.queries[self.transac_id]
                return False
            else:
        #del self.queries[self.transac_id]
                return False

    def commit(self):
        #index_for_query = self.queries.index((query, args))
        #print(index_for_query)
        #print(query.__name__, type(query.__name__))
        count = 0
        #print(self.committed_select_record_location,'update',self.committed_update_record_location)
        for key_,value in self.queries.items():
            for tuple in value:
                query = tuple[0]
                args_ = tuple[1]
                if query.__name__ == 'select':
                    query(*args_,transac_id_ =key_,loc_ = self.committed_update_record_location[self.transac_id][count//2], commit = 1, abort = None)
                else:
                    print('ready to commit', query.__name__, args_)
                    a = self.committed_update_record_location[self.transac_id][count//2]
                    print(a)
                    query(*args_,transac_id =key_, loc = a, commit__ = 1, abort = None)
                count+=1
        return True

        #del self.queries[index_for_query]
