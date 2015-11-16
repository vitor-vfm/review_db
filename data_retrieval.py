from bsddb3 import db
import re

# load up all four indicies/databases
reviewsDB = db.DB()
reviewsDB.open("rw.idx")

ptermsDB = db.DB()
ptermsDB.open("pt.idx")

rtermsDB = db.DB()
rtermsDB.open("rt.idx")

scoresDB = db.DB()
scoresDB.open("sc.idx")


class Query():
    """
    Represents a query, including all conditions

    The constructor parses a string and constructs lists
    of pterms, rterms, general terms, and conditions
    """
    def __init__(self, q):
        """
        Parse a query
        """
        self.pterms = []
        self.rterms = []
        self.generalterms = []
        # conditions are a list of tuples (term, operator, value)
        # e.g.: rscore > 4 -> ('rscore','>','4')
        self.conditions = []

        # find pterm specifications
        m = re.findall(r'p:([a-z]+)', q)
        if m:
            self.pterms += m
        q = re.sub(r'p:[a-z]+', '', q)

        # find rterm specifications
        m = re.findall(r'r:([a-z]+)', q)
        if m:
            self.rterms += m
        q = re.sub(r'r:[a-z]+', '', q)

        # find conditions
        m = re.findall(r'([a-z]+) ?([<>]) ?([0-9/]+)', q)
        if m:
            self.conditions += m
        q = re.sub(r'[a-z]+ ?[<>] ?[0-9/]+', '', q)

        # find general terms
        m = re.findall(r'[a-z%]+', q)
        if m:
            self.generalterms += m

def processQuery(query):
    """
    Get the results in the database specified 
    by the query object
    """
    # TODO: Get data from db according to query object
    return None



def interface():
    """
    Basic UI to ask for queries
    """
    while True:
        q = input("Please input your query. Input 'q' to exit: ").lower()
        if q == 'q':
            break
        query = Query(q)
        print(query.pterms)
        print(query.rterms)
        print(query.generalterms)
        print(query.conditions)
        res = processQuery(query)
        print(res)

    # close dbs before exiting
    reviewsDB.close()            
    ptermsDB.close()
    rtermsDB.close()
    scoresDB.close()            


if __name__=='__main__':
    interface()
