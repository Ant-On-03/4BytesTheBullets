class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

    def getIds(self):
        return self.id

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, seal, license, apc, publisher = None):
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.category = None
        self.area = None
        super().__init__(id)

    def __repr__(self):
        return f"Journal(id:{self.id}, title:{self.title}, languages:{self.languages}, apc:{self.apc}, publisher:{self.publisher}, category:{self.category}, area:{self.area})"
    
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
    def __init__(self, id, quartile = None):
        self.quartile = quartile
        super().__init__(id)

    def __repr__(self):
        return f"Category(id:{self.id}, quartile:{self.quartile})"
    
    def __str__(self):
        return f"Category(id:{self.id})"

    def getQuartile(self):
        return self.quartile
    
class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)

    def __repr__(self):
        return f"Area({self.id})"
    
    def __str__(self):
        return f"Area({self.id})"