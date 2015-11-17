from bsddb3 import db as berkeleyDB
import re

# load up all four indicies/databases
reviewsDB = berkeleyDB.DB()
reviewsDB.open("rw.idx")

ptermsDB = berkeleyDB.DB()
ptermsDB.open("pt.idx")

rtermsDB = berkeleyDB.DB()
rtermsDB.open("rt.idx")

scoresDB = berkeleyDB.DB()
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

    print(query.pterms) # product terms
    print(processPterms(query.pterms))

    print(query.rterms) # rating ex. great 
    print(processRterms(query.rterms))
    
    print(query.generalterms) # search product title, review summary and review text for term  
    print(processGeneralTerms(query.generalterms))

    print(query.conditions) # stored as tuples: (property, comparison, value) ex ('rscore', '>', '4')
    print(processConditions(query.conditions))

    # query results are 'AND'ed together 

    resultIDs = processPterms(query.pterms) + processRterms(query.rterms) + processGeneralTerms(query.generalterms) + processConditions(query.conditions)
    print(resultIDs)
    return None

def getAllMatchingKeys(masterKey, db):
    # http://stackoverflow.com/questions/12348346/berkeley-db-partial-match
    db_cursor = db.cursor()
    (key,value) = db_cursor.get(bytes(masterKey, encoding="utf-8"), berkeleyDB.DB_SET_RANGE)
    
    resultsIDs = [value]
    while(key == bytes(masterKey, encoding="utf-8")):
        (key,value) = db_cursor.get(bytes(masterKey, encoding="utf-8"), berkeleyDB.DB_NEXT)
        resultsIDs += [value]

    return resultsIDs
    
def processPterms(pterms):
    # uses ptermsDB
    resultIDs = []
    for pterm in pterms:
        # http://stackoverflow.com/questions/19511440/add-b-prefix-to-python-variable
        # resultIDs += [ptermsDB.get(bytes(pterm, encoding="utf-8"))] # already in encoded in bytes
        resultIDs += getAllMatchingKeys(pterm, ptermsDB)
    return resultIDs

def processRterms(rterms):
    # uses rtermsDB
    resultIDs = []
    for rterm in rterms:
        # http://stackoverflow.com/questions/19511440/add-b-prefix-to-python-variable
        # resultIDs += [rtermsDB.get(bytes(rterm, encoding="utf-8"))] # already in encoded in bytes
        resultIDs += getAllMatchingKeys(rterm, rtermsDB)
    return resultIDs

def processGeneralTerms(generalterms):
    # uses ptermsDB, rtermsDB
    resultIDs = processRterms(generalterms) + processPterms(generalterms)
    
    return resultIDs
def processConditions(conditions):
    resultIDs = []
    return resultIDs
    pass


def interface():
    """
    Basic UI to ask for queries
    """
    while True:
        q = input("Please input your query. Input 'q' to exit: ").lower()
        if q == 'q':
            break
        query = Query(q)
        res = processQuery(query)
        print(res)

    # close dbs before exiting
    reviewsDB.close()            
    ptermsDB.close()
    rtermsDB.close()
    scoresDB.close()            


if __name__=='__main__':
    interface()
