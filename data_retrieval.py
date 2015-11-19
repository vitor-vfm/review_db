#! /usr/bin/python3

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

        # These lists represent the maximum and minimum
        # for the range conditions 
        # The absence of an upper or lower bound
        # is represented as None
        self.rscoreBounds = [None, None]
        self.rdateBounds = [None, None]
        self.ppriceBounds = [None, None]

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

        # find bounds
        self.processBounds(re.findall(r'([a-z]+) ?([<>]) ?([0-9/]+)', q))
        q = re.sub(r'[a-z]+ ?[<>] ?[0-9/]+', '', q)

        # find general terms
        m = re.findall(r'[a-z%]+', q)
        if m:
            self.generalterms += m

    def processBounds(self, conditions):
        """
        Populate the Bounds tuples
        """
        for term, operator, bound in conditions:
            
            if term == 'rscore':
                bounds = self.rscoreBounds
            elif term == 'rdate':
                bounds = self.rdateBounds
            else:
                bounds = self.ppriceBounds

            if operator == "<":
                bounds[1] = bound
            else:
                bounds[0] = bound

# End of Query class

def processQuery(query):
    """
    Get the results in the database specified 
    by the query object
    """
    # TODO: Get data from db according to query object

    print("pterms: ",query.pterms) # product terms
    print("result: ",processPterms(query.pterms))

    print("rterms: ",query.rterms) # rating ex. great 
    print("result: ",processRterms(query.rterms))
    
    print("general terms: ",query.generalterms) # search product title, review summary and review text for term  
    print("result: ",processGeneralTerms(query.generalterms))

    print("rscore bounds: ",query.rscoreBounds)
    print("rdate bounds: ",query.rdateBounds)
    print("pprice bounds: ",query.ppriceBounds)

    # query results are 'AND'ed together 

    resultIDs = processPterms(query.pterms) + processRterms(query.rterms) + processGeneralTerms(query.generalterms) + processConditions()
    return resultIDs

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

def rangeSearch(term, maximum, minimum):
    # TODO: either use this function or delete it
    """
    Perform range searches
    """
    if term == 'rscore':
        db = scoresDB
    db_cursor = db.cursor()

    if not maximum:
        (maxKey,_) = db_cursor.get(berkeleyDB.DB_LAST)
    else:
        (maxKey,_) = db_cursor.get(bytes(maximum, encoding="utf-8"), berkeleyDB.DB_SET_RANGE)

    if not minimum:
        (minKey,value) = db_cursor.get(berkeleyDB.DB_FIRST)
    else:
        (minKey,value) = db_cursor.get(bytes(minimum, encoding="utf-8"), berkeleyDB.DB_SET_RANGE)

    result = []
    key = minKey
    while (key <= maxKey):
        result.append(value)
        (key,value) = db_cursor.get(berkeleyDB.DB_NEXT)
    return result


def processConditions():
    # TODO: either use this function or delete its contents
    """
    Process range conditions
    """
    return []
    # maxes = {}
    # mins = {}
    # for condTuple in conditions:
    #     if condTuple[1] == "<":
    #         maxes[condTuple[0]] = condTuple[2]
    #     elif condTuple[1] == ">":
    #         mins[condTuple[0]] = condTuple[2]
    # # get all terms removing duplicates
    # terms = set(list(maxes.keys()) + list(mins.keys()))

    # results = []
    # for term in terms:
    #     if term in maxes:
    #         ma = maxes[term]
    #     else:
    #         ma = None
    #     if term in mins:
    #         mi = mins[term]
    #     else:
    #         mi = None
    #     results += rangeSearch(term, ma, mi)
    # return results


def interface():
    """
    Basic UI to ask for queries
    """
    while True:
        q = input("Please input your query. Input 'q' to exit: ").lower()
        if q == 'q':
            break
        print(processQuery(Query(q)))

    # close dbs before exiting
    reviewsDB.close()            
    ptermsDB.close()
    rtermsDB.close()
    scoresDB.close()            


if __name__=='__main__':
    interface()
