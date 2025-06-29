import psycopg2
from psycopg2 import extras
from psycopg2.extras import Json


# class that wraps the db and has two jobs
# 1. get the DOIS from the AB_papers table OR the AB_referrers table 

#ab_referrers
# animal_behaviour
ab = "SELECT doi FROM animal_behaviour ORDER BY doi OFFSET %s LIMIT %s;"
#DISTINCT on(doi)
ab_refs = """
SELECT DISTINCT on(doi) doi FROM ab_referrers
WHERE position('0003-3472(' in doi) =0
AND position('S0950-5601(' in doi) =0
AND position('j.anbehav' in doi) =0
AND position('anbe.199' in doi)= 0 
AND position('anbe.200' in doi) = 0
ORDER BY doi
OFFSET %s LIMIT %s;"""

affiliations_create="CREATE TABLE IF NOT EXISTS paper_affiliations(doi VARCHAR, name VARCHAR, city VARCHAR, country VARCHAR);"
affiliations_insert="INSERT INTO paper_affiliations(doi, name, city, country) VALUES %s;"

abstract_create="CREATE TABLE IF NOT EXISTS abstract_log(type VARCHAR, start INT, off INT, body JSONB);"
abstract_insert="INSERT INTO abstract_log(type, start, off, body) VALUES (%s, %s, %s, %s);"
abstract_select = "SELECT * FROM abstract_log ORDER BY start DESC LIMIT 1;"


log = "INSERT INTO scopus_log(type, start, off, body) VALUES (%s, %s, %s, %s);"
createLog = "CREATE TABLE IF NOT EXISTS scopus_log(type VARCHAR, start INT, off INT, body JSONB);"
selectLog = "SELECT * FROM scopus_log ORDER BY id DESC LIMIT 1;"


stmts = {
    'ab_select':ab,
    'refs_select':ab_refs,
    'log_insert':log,
    'log_create':createLog,
    'log_select':selectLog,
    'affiliations_create':affiliations_create,
    'affiliations_insert':affiliations_insert
}


class Wrapper:
    def __init__(self, stmts, db='postgres://ed@localhost/references' ):
        self.con=None
        self.stmts = stmts
        self.db = db

    def start(self):
        # open connnection
        try:
            self.con = psycopg2.connect(self.db)
            cur = self.con.cursor()
           # cur.execute(self.stmts.get("affiliations_create"))
            
            cur.execute(self.stmts.get("log_create"))
            self.con.commit()
        except Exception as e:
            print("Failed to connect to DB and ensure the required tables exist %s" % str(e) )
            



    def select(self, table, offset, limit):
        cur = self.con.cursor()
        try:
            if table =="animal_behaviour":
                cur.execute(self.stmts.get("ab_select"), [offset, limit])
                return cur.fetchall()

            if table=="refs":
                cur.execute(self.stmts.get("refs_select"), [offset, limit])
                return  cur.fetchall()

        except Exception as e:
            print("SELECT FAILED: %s" % str(e))
            # commit otherwise blocked
            self.con.commit()
            return []

        return []

    def log(self, type, start, offset, body ):
        try:
            cur = self.con.cursor()
            # original query
            cur.execute(self.stmts.get("log_insert"), (type, start, offset, Json(body)))
            
            #cur.execute(self.stmts.get("log_insert"), (Json(body),))
            self.con.commit()
        except Exception as e:
            print("ERROR logging JSON: %s" % str(e))
        return
        #

    def insert_affiliations(self, arr):
        try:
            cur= self.con.cursor()
            extras.execute_values(cur, self.stmts.get("affiliations_insert"), arr, template='(%s, %s, %s, %s)', page_size=100 )
            self.con.commit()
        except Exception as e:
            print("error inserting affiliations: %s" %str(e))
        return

    def lastLog(self):
        cur=self.con.cursor()
        cur.execute(self.stmts.get("log_select"))
        return cur.fetchone()


    
wrappedDB = Wrapper(stmts)

abstractStmts = {
    
     'ab_select':ab,
    'log_insert':abstract_insert,
    'log_create':abstract_create,
    'log_select':abstract_select,

    

}
#wrappedAbstractDB = Wrapper(abstractStmts)
