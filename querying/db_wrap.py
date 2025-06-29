

import psycopg2

from species_extraction import SpeciesChecker
from speciesHandler import SpeciesHandler


db='postgres://ed@localhost/references'

class DB_WRAP:
    def __init__(self, db):
        self.db = db
        self.con = None
        self.cur =None
    
    def start(self):
        try:
            self.con = psycopg2.connect(self.db)

            self.cur = self.con.cursor()
                   
            return True

        except Exception as e:
            print("Failed to connect to DB and ensure the required tables exist %s" % str(e) )
            return False

    def attempt(self, stmt, vars=None, fetch = True):
        if vars:
            self.cur.execute(stmt, vars)

        else:
            self.cur.execute(stmt)
        if fetch:
            return self.cur.fetchall()
        
        return []

    


    def fromStatement(self, stmtPath, vars=None, fetch = True):
        with open(stmtPath, "r") as f:
            stmt = f.read()
            if not vars:
                return self.attempt(stmt, fetch=fetch)

            vars = []
            for word in stmt.split(" "):
                if len(word) and word[0] ==":":
                    vars.append(word)

            for var in vars:
                stmt = stmt.replace(var, f"%({var[1:]})s")



            return self.attempt(stmt, vars, fetch)





class AB_QUERY(DB_WRAP):

    def __init__(self, db, outputs={}):
        super().__init__(db)
        self.paper_type = None
        self.from_year = None
        self.outputs = outputs

    def set_ab_papers_post_1989(self):
        
        self._drop_selection_view()
        self.fromStatement("./sql_statements/setup/ab_paper_1989_view.sql", fetch=False)
        self._selectAB()
        self._selectPost1989()

    def set_ab_papers_all_history(self):
        
        self._drop_selection_view()
        self._selectAB()
        self._selectAllHistory()
        self.fromStatement("./sql_statements/setup/ab_paper_all_view.sql",  fetch=False)

    def set_ab_references_to_papers_post_1989(self):
        
        self._drop_selection_view()
        self._selectReferrers()
        self._selectPost1989()
        self.fromStatement("./sql_statements/setup/ab_referrers_1989_view.sql",  fetch=False)


    def set_ab_references_to_papers_all_history(self):

        self._drop_selection_view()
        self._selectReferrers()
        self._selectAllHistory()
        self.fromStatement("./sql_statements/setup/ab_referrers_all_view.sql",  fetch=False)
    
    def _drop_selection_view(self):

        self.fromStatement("./sql_statements/setup/drop_selected_paper_view.sql", fetch=False)
        
        
    def _selectAB(self):
        self.paper_type = "ANIMAL_BEHAVIOUR_PAPERS"

    def _selectPost1989(self):
        self.from_year = 1989

    def _selectAllHistory(self):
        self.from_year = 1950

    def _selectReferrers(self):
        self.paper_type = "REFERRERS"

    def n_papers(self):
        return self.fromStatement("./sql_statements/countDistinctPapers.sql")

    def authors_per_paper(self):
        """

        Executes sql statement returning the mean and standard derviation of number of authors per paper on the selected view

        """

        return self.fromStatement("./sql_statements/authors_per_paper_ab.sql" )

    def total_n_countries(self):
        """
        
        Executes sql statement returning the total number of countries from authors in the selected view

        """
        return self.fromStatement("./sql_statements/location_info/total_countries.sql" )

    def top_countries(self):
        """
        
        Executes sql statement returning the number of authors per countrys -- currently Limits to the 10 countries with the most authors

        """

        return self.fromStatement("./sql_statements/location_info/overall_countries.sql" )


    def top_continents(self):
        """
        
        Executes sql statement returning the number of authors per continent

        """

        return self.fromStatement("./sql_statements/location_info/overall_continents_per_paper.sql")


    def total_n_institutions(self):
        """
        
        Executes sql statement returning the total number of institutions on the selected view

        """
        return self.fromStatement("./sql_statements/intitution_info/total_unique_institutions.sql")

    def top_institutions(self):
        """
        
        Executes sql statement returning the institutions with the highest number of authors in the selected view
        """
        return self.fromStatement("./sql_statements/intitution_info/highest_author_institutions.sql")


    def top_species(self):
        """
        **********

        """
        pass

    def n_distinct_species(self):
        """

        ***********
        

        """
        pass

    def get_candidate_species(self, limit=10, offset=1):

        """
        Extracts candidate species based on matches in titles and abstract
        
        """


        qry = """
                SELECT JSON_AGG(canonical_name) candidates, any_value(details), doi FROM 
                (
                    SELECT a.canonical_name, (p.body#>>'{published, date-parts, 0, 0}')::int pub_year, concat(p.body#>>'{title, 0}', ' ', abstrct.abstract) details, p.doi doi  FROM 
                                (       SELECT DISTINCT ON (doi, canonical_name) * FROM title_latin_animals 
                                        UNION SELECT * FROM abstract_latin_animals) as a
                                        JOIN paper_stage as p ON p.doi = a.doi
                                        JOIN ab_abstracts as abstrct ON abstrct.doi = p.doi

                                ) WHERE pub_year > %s

                GROUP BY doi
                LIMIT %s OFFSET %s ;

                """ % (self.from_year, limit, offset)

        return self.attempt(qry)

    def get_species(self):


        """
        Checks and avoids wrongly matching candidate species 
        
        """
        sc = SpeciesChecker()
        sh = SpeciesHandler(sc, self, 5000)
        return sh.run()

    def runQueries(self):

        formatted_results = []
        
        for k,v in self.outputs.items():
            fn = getattr(self, k)
            formatted_results.append(v(fn()))

        return formatted_results
        


"""

## Example of how to run queries

the object ab_query needs to be started to connect to the database
then you need to set which papers you are interested in references or orginal papers and the year

ab_query = AB_QUERY(db)

ab_query.start()
ab_query.set_ab_references_to_papers_post_1989()

print(ab_query.get_species())


"""