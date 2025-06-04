class IdentifiableEntity(object):
    """
    Base class for all entities that have an ID.
    
    """
    def __init__(self, id):
        self.id = id

    def getIds(self) -> list:
        return self.id

class Journal(IdentifiableEntity):
    """Class representing a journal.
    
    Attributes:
    id (list[string]): The ISSN and/or EISSN ID of the journal
    title (string): The title of the journal 
    languages (string): The languages in which the journal is published
    seal (bool): A boolean to state whether the journal has a DOAJ seal 
    license (string): The licenses the journal has
    apc (bool): A boolean to state whether the Article Processing Charge (APC) has been paid for the journal 
    publisher (string): The institution which publishes the journal 
    category (list[Category]): The categories which the journal is published under
    area (list[Area]): The areas which the journal is published under
     
    """
    def __init__(self, id, title, languages, seal, license, apc, publisher = None):
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = True if seal == "Yes" else False
        self.license = license
        self.apc = True if apc == "Yes" else False
        self.category = None
        self.area = None
        super().__init__(id)

    def __repr__(self):
        return f"Journal(id:{self.id}, title:{self.title}, languages:{self.languages}, publisher:{self.publisher}, license:{self.license}, seal:{self.seal}, apc:{self.apc}, category:{self.category}, area:{self.area})"
    
    def __str__(self):
        return f"Journal(id:{self.id}, title:{self.title})"

    def getTitle(self):
        return self.title

    def getLanguages(self):
        lan = []
        for l in self.languages.split(', '):
            lan.append(l)
        
        return lan
    
    def getPublisher(self):
        return self.publisher
    
    def hasDOAJSeal(self):
        return self.seal

    def getLicence(self):
        return self.license
    
    def hasAPC(self):
        return self.apc

    def setCategories(self, categories):
        self.category = categories
        return True
    
    def getCategories(self):
        return self.category

    def setAreas(self, areas):
        self.area = areas
        return True 
    
    def getAreas(self):
        return self.area

class Category(IdentifiableEntity):
    """Class representing a category.
    
    Attributes:
    id (string): The name of the category 
    quartile (string): The quartile in which this category appears (based on the journals published under the category)
    journal_quartile (dict): A dictionary of journal:quartile pairs related to the category 
     
    """
    def __init__(self, id, journal_quartile:dict  = {}, quartile = None):
        self.journal_quartile = journal_quartile
        self.quartile = quartile
        super().__init__(id)

    def __repr__(self):
        return f"Category({self.id}, {self.quartile})"
    
    def __str__(self):
        return f"Category({self.id}, {self.quartile})"
    
    def getQuartile(self):
        return self.quartile
    
    def setQuartile(self, newQuartile):
        self.quartile = newQuartile
        return True
    
    def setJournalQuartile(self, journal_quartile):
        self.journal_quartile = journal_quartile
        return True

    def getJournalQuartile(self):
        return self.journal_quartile
    

     
    def getQuartileWithJournal(self, journal_id):
        return self.journal_quartile[journal_id]
    

    def setQuartileWithJournal(self, journal_id):
        self.setQuartile(self.getQuartileWithJournal(journal_id))
        return True
    
    def clean(self):
        self.journal_quartile = None
        return True
    

    
class Area(IdentifiableEntity):
    """Class representing a category.
    
    Attributes:
    id (string): The name of the area 
    journal (list): The list of journals published under the area 
     
    """
    def __init__(self, id, journal = []):
        self.journal = journal
        super().__init__(id)

    def __repr__(self):
        return f"Area({self.id})"
    
    def __str__(self):
        return f"Area({self.id})"
    
    def setJournal(self, journal):
        self.journal = journal
        return True

    def getJournal(self):
        return self.journal
