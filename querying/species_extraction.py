



class SpeciesChecker:

    """
    Class ensures no incorrect duplication of species names when one name appears in another

    e.g. both gallus gallus domesticus and gallus domesticus will appear as candidates for gallus gallus domesticus

    It orders candidate names by length (longest first) and then starting with the longest candidate name it \n
    replaces matching parts of the text with a none word filler to avoid shorter candidates incorrectly appearing as a match

    The results variable is a dict that provides a count of the papers in which the species appeared [key = species name, value = count]
    
    """



    def __init__(self, replacementString = "_____"):
        self.results = []
        self.inputs = []
        self.pos = 0
        self.results = {}
        self.replacementString = replacementString

    def run(self):
        pass

    def incrementPos(self):
        self.pos+=1

    def addItem(self, item):
        self.inputs.append(item)

    def addItems(self, items):
        self.inputs = self.inputs+items


    def current_item(self):
        return self.inputs[self.pos]

    def getCandidateSpecies(self):
        candidates = self.current_item()[0]
        return candidates
    def getText(self):
        return self.current_item()[1] #.get("text", "")
       

    def sort_targets_by_length(self):

        self.getCandidateSpecies().sort(key=lambda s:len(s), reverse=True)

    def extract_items(self):
        while self.pos < len(self.inputs):
            self.extract_item()
            self.incrementPos()
        
        # flush inputs and flag completion
        return self.complete()

        
    def complete(self):
        self.pos = 0
        self.inputs = []
        return "COMPLETE"
        

    def extract_item(self):

        self.sort_targets_by_length()

        candidates  = list(map(lambda a :a.lower(), self.getCandidateSpecies()))
        text = self.getText().lower()

        if len(candidates) ==1:
            self.incrementCandidate(candidates[0])
            return

        for candidate in candidates:
            if candidate in text:
                text = text.replace(candidate, self.replacementString )
                self.incrementCandidate(candidate)
 
    def incrementCandidate(self, candidate):
        curr = self.results.get(candidate, 0)
        self.results[candidate]= curr+1
