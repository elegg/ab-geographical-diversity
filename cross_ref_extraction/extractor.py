import psycopg2

from psycopg2 import extras
import gzip
import json
import glob
import os
import time
from enum import Enum
from psycopg2.extras import Json
from queue import Queue

#local path is just /postgre , hdd is /references
# ensure server is started brew services start postgresql@16
con = psycopg2.connect('postgres://ed@localhost/references')

cur = con.cursor()
"""

"""

"""
"Volumes/ed-hdd/2024-qb/april/april"
directory = '/volumes/ed-hdd/2024-qb/april/april'

items = list(filter(lambda a: not "_" in a, glob.glob(f'{directory}/*.json.gz')) )

def getJsonFileNames(directory):
    # crude filter of file with _ in to remove hidden files
    return list(filter(lambda a: not "_" in a, glob.glob(f'{directory}/*.json.gz')) )

def openFile(file):
    with gzip.open(file, "rt") as f:
        expected_dict = json.load(f)
        return expected_dict.get("items")



    

def selectAll():

    cur = con.cursor()
    cur.execute("SELECT paper->'DOI' FROM reference_import LIMIT 5;")
    res = cur.fetchall()
    print(res)
    con.commit()


#selectAll()
#print(conts)


def JsonArrayTest(cur, el):
    cur.execute("insert into reference_import (paper) VALUES (%s)", [ Json(el) ] )


def handlePage(items):

    cur = con.cursor()
    for item in items:
        JsonArrayTest(cur, item)
    con.commit()

 


def inserts():

    pages = list(filter(lambda a: not "_" in a, glob.glob(f'{directory}/*.json.gz')) )
    items = openFile(pages[0])
    handlePage(items)
    ##selectAll()

#inserts()
#selectAll()

"""
extras.execute_values (
    cursor, insert_query, data, template=None, page_size=100
)

"""

status = Enum("Status", ["SUCCESS", "FAIL", "INSERT_ERROR"])


def page_reader(page):
    status = Enum("Status", ["SUCCESS", "FAIL", "INSERT_ERROR"])
    try:
        contents= openFile(page)
        return {"type":status.SUCCESS.name, "payload":{"page":page, "papers":contents} }
    except:
       return {"type":status.FAIL.name, "payload":{"page":page}} 
    



class PaperWriter:
    def __init__(self, con, deps):
        self.con=con
        self.insert_papers = deps.get("paper")
        self.insert_page = deps.get("page")
        self.check_page_table =deps.get("create_page_table")
        self.check_paper_table =deps.get("create_paper_table")
        self.threshold=deps.get("threshold")
        self.successes =[]
        self.pages =[]
        self.targets={}
        self._finished = False

    def start(self, targets):
        self.con = psycopg2.connect('postgres://ed@localhost/references')
        cur = self.con.cursor()
        cur.execute(self.check_page_table)
        cur.execute(self.check_paper_table)
        self.con.commit()

        self.targets=set(targets)




    def handlePage(self, page):
        
        payload = page.get("payload")
        
        if page.get("type") ==status.FAIL.name: 
            self.handleFailedPage(payload.get("page"))

        if page.get("type") ==status.SUCCESS.name:
            self.handleReadPage(payload.get("page"), payload.get("papers"))

        # note: 5000 is number of papers per crossRef json file
        if len(self.successes) >= self.threshold+5000:
            self.makeInserts()

        #self.targets.remove(payload.get("page"))

        #if(not len(self.targets)):
            #self.close()

           
    def handleReadPage(self, page, papers):
        self.successes =self.successes + [(i["DOI"], Json(i)) for i in papers]
        self.pages.append((page, status.SUCCESS.name))
        

    def handleFailedPage(self, page):
        self.pages.append((page, status.FAIL.name))

    def makeInserts(self):
        cur =  self.con.cursor()

        try:
            extras.execute_values(cur,self.insert_papers, argslist=self.successes, template=None, page_size=100 )
        except Exception as e:
            print("FAILED TO INSERT PAPERS: %s", str(e))
            # first make commit to overwrite error
            self.con.commit()
            self.updatePageFail()

        try:
            extras.execute_values(cur, self.insert_page, argslist=self.pages, template=None, page_size=100)
        except Exception as e:
            print('Failed to Write pages: %s', str(e))

        self.con.commit()
        self.flush()
        print("...committed")

    def flush(self):
        self.successes=[]
        self.pages=[]
        
    def updatePageFail(self):
        for page in  self.pages:
            if(page[1] ==status.SUCCESS):
                page[1] = status.INSERT_ERROR.name

    def close(self):
        # make final insers to clean up
        if(len(self.pages)):
            self.makeInserts()
        
        self._finished=True

    def isFinished(self):
        return self._finished

#"CREATE TABLE IF NOT EXISTS page_stage (page VARCHAR, status VARCHAR, UNIQUE(page));",
#"CREATE TABLE IF NOT EXISTS paper_stage (doi VARCHAR, body JSONB, UNIQUE(doi));"
stmts = {
    "paper":"INSERT INTO paper_stage VALUES %s;",
    "page":"INSERT INTO page_stage values %s;",
    "create_page_table":"CREATE TABLE IF NOT EXISTS page_stage (page VARCHAR, status VARCHAR, UNIQUE(page));",
    "create_paper_table":"CREATE TABLE IF NOT EXISTS paper_stage (doi VARCHAR, body JSONB, UNIQUE(doi));"
}

"""

def jsonReading():
    pages = list(filter(lambda a: not "_" in a, glob.glob(f'{directory}/*.json.gz')) )
    #items = openFile(pages[0])
    cur = con.cursor()
    insertQ = "INSERT INTO test_import VALUES %s;"
    
    data = []
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS import_tmp (json JSONB);")
    cur.execute("CREATE TABLE IF NOT EXISTS test_import (doi VARCHAR, body JSONB, UNIQUE(doi));")

    for i in range(2):

        items = openFile(pages[i])
        data = data + [(i["DOI"], Json(i)) for i in items]
        
        #cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS import_tmp (json JSONB);")
        #cur.execute("INSERT INTO import_tmp VALUES (%s);", [Json([items])])  
    
    extras.execute_values(cur,insertQ, argslist=data, template=None, page_size=100 )
    
    cur.execute("SELECT doi FROM test_import LIMIT 50;")  
    res = cur.fetchall()
    print(res)
    con.commit()
"""
## 30000 seems optimal for my laptop
stmts.update({"threshold":0})


def paperWorker(files, q):
    pw = PaperWriter(None, stmts)
    pw.start(files)
    while not pw.isFinished():
        paper = q.get()
        #print(paper)
        pw.handlePage(paper)

    return "FINISH"


def readerWorker(qs):
    inQ, outQ = qs
    while not inQ.empty():
        res = inQ.get()
        page = page_reader(res)
        outQ.put(page)
    return


def fileSelector(start, finish):
    return getJsonFileNames(directory)[start:finish]

t0 = time.time()
files = getJsonFileNames(directory)[300:310]
Q= Queue()

def paperWorker2(files, q):
    pw = PaperWriter(None, stmts)
    pw.start(files)
    while not pw.isFinished():
        res = q.get()
        paper = page_reader(res)
        pw.handlePage(paper)
    return "FINISH"

[Q.put(i) for i in files]





def paperWorker3(deps):
    files, q = deps
    pw = PaperWriter(None, stmts)
    pw.start(files)
    while not q.empty():

        res = q.get()        
        paper = page_reader(res)
        pw.handlePage(paper)
    pw.close()

    return "FINISH"

#paperWorker2(files, Q)
#print(time.time()-t0)




def select():
    con = psycopg2.connect('postgres://ed@localhost/references')
    cur=con.cursor()
    cur.execute("SELECT COUNT(*) FROM paper_stage;")
    #cur.execute("DROP TABLE paper_stage;")
    
    #cur.execute("DROP TABLE page_stage;")

    res = cur.fetchall()
    print(res)

#select()



## INSERTS INTO ANIMAL_BEHAVIOUR TABLE
"""
INSERT INTO animal_behaviour (doi, type) SELECT doi, 'paper' FROM paper_stage WHERE position('j.anbehav.' in doi)>0;
"""

"""
INSERT INTO animal_behaviour (doi, type) SELECT doi, 'paper' FROM 
paper_stage WHERE position('0003-3472(' in doi)>0 OR position('S0950-5601(' in doi)>0 ;
"""

"""INSERT INTO animal_behaviour (doi, type) SELECT doi, 'paper' FROM 
paper_stage WHERE position('anbe.199' in doi)>0 OR position('anbe.200' in doi)>0;"""



examples = [
    {"DOI":"1234target", "references":[{"DOI":"abcd"}]},
    {"DOI":"abcd", "references":[{"journal":"xyx"}]},
    {"DOI":"5678target"},
    {"DOI":"fghk", "references":[{"DOI":"1234target"}, {"DOI":"abcd"}]}
    ]

"""SELECT ?? FROM papers, json_b_elements(references)
select * from jsonb_path_query('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}')

"""


def makeSelect(stmt):
    con = psycopg2.connect('postgres://ed@localhost/postgres')
    cur=con.cursor()
    cur.execute(stmt)
    #cur.execute("DROP TABLE paper_stage;")
    
    #cur.execute("DROP TABLE page_stage;")

    res = cur.fetchall()
    print(res)




t = """jsonb_path_query(
        '{"a":[{"references":["ref1", "ref2"]}]}',
         '$.a[*].references[*]'
         )"""
jbq = """
    select ref FROM (
    select * from 
    jsonb_path_query(
        '{"a":[{"references":["ref1", "ref2"]}]}',
         '$.a[*].references[*]'
    
         )
         ) as b(id, ref) WHERE 
         position('1' in b.ref ->> 0) >0
         OR position('2' in b.ref ->> 0) >0
         
         ;
         """




j2 = """SELECT ('{"scores": [1,2,3,4,5]}'::jsonb @@ '$.scores[*] > 2') result;"""

j3 = """select data from jsonb('{"a":[{"references":["ref1", "ref2"]}]}') as data WHERE data @@ 'ref1 in $.a[*].references' ;"""

j4 = """SELECT body FROM 
        (SELECT jsonb_path_query(body, '$.reference[*]') FROM jsonb('{"reference":[{"DOI":"1"}]}') as body )
         as ref(body) 
          WHERE position('1' in ref.body->>'DOI')>0 LIMIT 5
        ;"""


"""SELECT doi, page.refs->>'DOI' INTO ab_referrers FROM 
        (SELECT doi, jsonb_path_query(body, '$.reference[*]') FROM paper_stage )
         as page(doi, refs) WHERE
           position('0003-3472(' in page.refs ->> 'DOI') >0
         OR position('S0950-5601(' in page.refs ->> 'DOI') >0
         OR position('j.anbehav' in page.refs ->> 'DOI') >0
;"""

"""
INSERT  INTO ab_referrers SELECT doi, page.refs->>'DOI' FROM 
        (SELECT doi, jsonb_path_query(body, '$.reference[*]') FROM paper_stage )
         as page(doi, refs) WHERE
           position('anbe.199' in doi)>0 
           OR position('anbe.200' in doi)>0;
"""
makeSelect(j4)


# for animal behaviour there are multiple journal doi prefixes:
# 0003-3472
# j.anbehav
# S0950-5601 (when it was british journal of animal behaviour
# 
# 
# 
# 
# 
# 
#  SELECT animal_behaviour.doi, body FROM paper_stage JOIN animal_behaviour ON animal_behaviour.doi=paper_stage.doi LIMIT 1;
# 
# )


"""

SELECT doi, jsonb_path_query(body, '$.reference[*].DOI') as ref FROM paper_stage WHERE position('10' in body->>0) >0 LIMIT 5;

SELECT doi, jsonb_path_query({"reference":[{"DOI":1}]}, '$.reference[*].DOI') as ref FROM paper_stage WHERE position('1' in body->>0) >0 LIMIT 5;
    
         )) as b(id, ref) WHERE 
         position('1' in b.ref ->> 0) >0
         OR position('2' in b.ref ->> 0) >0



         position('0003-3472(' in ref.doi ->> 0) >0
         OR position('S0950-5601(' in ref.doi ->> 0) >0
         OR position('j.anbehav' in ref.doi ->> 0) >0
         
         ;

"""


# 
#  SCOPUS END POINT:
# https://api.elsevier.com/content/search/scopus?httpaccept=application/json&query=diabetes&apiKey=[Your API Key]
# 
# 
# facets=prefnameauid(count=20,sort=na,prefix=Ma);exactsrctitle(prefix=J);subjabbr(sort=fd);pubyr;exactkeywords(sort=fdna)
# 

#facets = authname(count=20);af-id(count=50);country(count=50)
# facets	xsd:string
#options: af-id, aucite, au-id, authname, country, exactsrctitle, fund-sponsor, language, openaccess, pubyear, restype, srctype, subjarea
#   sdsd 


# https://api.elsevier.com/content/search/scopus?httpaccept=application/json&query=diabetes&apiKey=[Your API Key]

"""
curl -X GET --header 'Accept: application/xml' 'https://api.elsevier.com/content/search/scopus?httpaccept=application/json&query=https://doi.org/10.1016/j.anbehav.2023.10.013&
&a

"""