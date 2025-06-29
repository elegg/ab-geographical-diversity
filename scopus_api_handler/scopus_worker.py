
import time

# 1. request 25 items from AB database & increment offset. IF NO ROWS move to AB_referrers
# 2. send API request to SCOPUS
# 3. IS API request 200? DUMP JSON in DB. IF 429 ABORT else ERROR
# 4. IS request N > 0 ? continue : ABORT
# 5. FINISH
# 
# 

class AffiliationRequester:
    def __init__(self,dbWrapper, reqHandler, extractEntries, dbs=[ {"name":"animal_behaviour", "complete":False}], pageLength=25):
        self.dbs =dbs
        self.pageLength = pageLength
        self.currentPos = 0
        self.currentPage = None
        self.dbWrapper = dbWrapper
        self.reqHandler = reqHandler
        self.extractEntries=extractEntries
        self.ABORT = False

    def start(self):
        self.dbWrapper.start()
        lastLog = self.dbWrapper.lastLog()
        
        if lastLog:
            table, start, offset, body, id =lastLog

            for db in self.dbs:
                if db["name"] == table:
                    self.currentPos=0#overode becasue of error start+offset 
                    self.currentPage=table
        
        else:
            self.currentPos=0
            self.currentPage = self.currentDB()
        
        #self.currentPos=0
        #self.currentPage = self.currentDB()
        
        self.run()
            


    def isDBFinished(self):
        for db in self.dbs:
           if not db.get("complete"):
               return False

        return True

    def currentDB(self):
         for db in self.dbs:
           if not db.get("complete"):
               self.currentPage=db.get("name")
               return db.get("name")
    
    def finishDB(self, curr):
        dbs =[]
        for i in self.dbs:
            if i.get("name")==curr:
                i.update({"complete":True})
            dbs.append(i)
        self.currentPos=0
    
    def run(self):
        
        while not (self.isDBFinished() or self.ABORT) :
            currDb = self.currentDB()
            print(currDb, self.currentPos)
            res = self.getDBPage()

            if(len(res)):
                self.currentPos+=self.pageLength
                self.handleRequest(res)
            else:
                self.finishDB(currDb)

            # likely unnecessary but ensure requests are at a lower rate than required
            time.sleep(0.15)

        print("FINISHED")

    def handleRequest(self, rows):
        status, res = self.reqHandler(rows).values()
        # no point doing anything if no response
        if(len(res)):
           self.dbWrapper.log(self.currentPage, self.currentPos, self.pageLength, res)
           #self.dbWrapper.log(res)

            # EWL TOOK OUT 8th November TO MAKE A simple log of scopus abstracts
            #self.dbWrapper.insert_affiliations(self.extractEntries(res))
        if status=="ABORT":
            self.ABORT=True
        return 


    def getDBPage(self):

       offset =  self.currentPos
       limit = self.pageLength

       return self.dbWrapper.select(self.currentPage, offset, limit)




def extractEntries(res):
    values = []
    if ("search-results" in res) and ("entry" in res.get("search-results")):
        entries = res.get("search-results").get("entry")
        doi = None
        
        for entry in entries:
            doi = entry.get("prism:doi")
            for aff in entry.get("affiliation") or []:
                name = aff.get("affilname")
                city = aff.get("affiliation-city")
                country = aff.get("affiliation-country")
                values.append((doi, name, city, country))

    return values


