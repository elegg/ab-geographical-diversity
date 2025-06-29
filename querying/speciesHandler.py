
class SpeciesHandler:

    def __init__(self, species_checker, dbWrapper, page_size=100):
        self.species_checker = species_checker
        self.dbWrapper = dbWrapper
        self.pos = 1
        self.page_size = page_size


    def run(self):

        while True:
            res=  self.requestNextItems()
            if len(res):
                self.species_checker.addItems(res)
                self.species_checker.extract_items()
                self.pos+=self.page_size
                continue

            else:
                break

        return self.species_checker.results


    def requestNextItems(self):
        self.offset = self.pos
        limit = self.page_size
        print(self.offset, flush = True)

        return self.dbWrapper.get_candidate_species(offset=self.pos, limit=limit)

        ## perfom query self.dbWrapper
        


    def save_as_json(self):
        ## 
        pass

