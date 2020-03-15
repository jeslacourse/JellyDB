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
        for key,value in self.queries.items():
            for tuple in value:
                query = tuple[0]
                args = tuple[1]
                if query.__name__ == 'select':
                    result = query(*args, transac_id_ = key,loc_ = None, commit=None, abort=None)
                else:
                    result = query(*args, transac_id = key,loc = None, commit=None, abort=None)
                # If the query has failed the transaction should abort
                # writing queries returns record location
                #print(self.committed_update_record_location, threading.current_thread().name)
                if result == False:
                    return self.abort(query, *args, transac = key)
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

    def abort(self,query,*args,transac):
        if query.__name__ == 'select':
            #print(self.committed_select_record_location)
            if transac in self.committed_select_record_location.keys():
                for items in self.committed_select_record_location[transac]:
                    query(*args,transac_id_ = None, loc_ = items, commit = None,abort = 1)
                #print('abort status check',self.committed_select_record_location)
                del self.committed_select_record_location[transac]
                del self.committed_update_record_location[transac]
                del self.queries[transac]
                return False
            else:
                return False
        else:
            if transac in self.committed_update_record_location.keys():
                #print(self.committed_update_record_location,'this is update')

                for items in self.committed_update_record_location[transac]:
                    query(*args,transac_id = None,loc = items, commit = None, abort = 1)
                #print('abort status check', query.__name__,self.committed_select_record_location)
                del self.committed_update_record_location[transac]
                del self.committed_select_record_location[transac]
                del self.queries[transac]
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
        for key,value in self.queries.items():
            for tuple in value:
                query= tuple[0]
                args = tuple[1]
                if query.__name__ == 'select':
                    #print('++++++++++++++++++ready to commit select', query.__name__, *args)
                    result = query(*args,transac_id_ =None,loc_ = self.committed_select_record_location[self.transac_id][count//2], commit = 1, abort = None)
                else:
                    #print('++++++++++++++++++++ready to commit increment',key, query.__name__, *args)
                    result = query(*args,transac_id =key, loc = self.committed_update_record_location[self.transac_id][count//2], commit = 1, abort = None)
                    #print(result,'should have finished')
                count+=1
        return True

        #del self.queries[index_for_query]
