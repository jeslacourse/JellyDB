from JellyDB.table import Table, Record
import inspect
from JellyDB.query import Query
#from JellyDB.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

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
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            # writing queries returns record location
            if result == False:
                return self.abort(query, args)
            else:
                return self.commit(query, args, result)

    def abort(self,query,args):
        #TODO: do roll-back and any other necessary operations
        index_for_query = self.queries.index((query, args))
        del self.queries[index_for_query]
        return False

    def commit(self,query, args, result_location):
        # TODO: commit to database
        index_for_query = self.queries.index((query, args))
        if query.__name__ == 'increment':
            result = query(*args, loc = result_location, commit = True)
        else:
            pass
        del self.queries[index_for_query]
        return True
