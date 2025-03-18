import rdflib as rdf
import pandas as pd

class Handler(object):
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl
        return True

class UploadHandler(Handler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def pushDataToDb(self, path):
        pass

class JournalUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl):
       #create graph to hold journal triples
       self.graph=rdf.Graph()
      
       #add URI for attributes
       self.Journal = "https://schema.org/Periodical"
       self.id_issn = rdf.URIRef("https://schema.org/identifier/issn")
       self.id_eissn = rdf.URIRef("https://schema.org/identifier/eissn")
       self.title = rdf.URIRef("https://schema.org/name")
       self.language = rdf.URIRef("https://schema.org/language")
       self.publisher = rdf.URIRef("https://schema.org/publisher")
       self.seal = rdf.URIRef("https://schema.org/award")
       self.license = rdf.URIRef("https://schema.org/license")
       self.apc = rdf.URIRef("https://schema.org/apc")

       super().__init__(dbPathOrUrl)


    def addTriples(self):
        # memo for later: if necessary, change non-values to empty strings, and define dtype
        # check for values before making triples (if empty, what should we do?)
        # check for one missing publisher
        # check how to deal with languages        
        journals = pd.read_csv(self.dbPathOrUrl)  
        for idx, row in journals.iterrow():
           local_id = "journal-" + str(idx)
           subj = rdf.URIRef(self.Journal + local_id)
           self.graph.add((subj, self.id_issn, rdf.Literal["Journal ISSN (print version)"]))  
           self.graph.add((subj, self.id_eissn, rdf.Literal["Journal EISSN (online version)"])) 
           self.graph.add((subj, self.title, rdf.Literal["Journal title"])) 
           self.graph.add((subj, self.language, rdf.Literal["Languages in which the journal accepts manuscripts"])) 
           self.graph.add((subj, self.publisher, rdf.Literal["Publisher"])) 
           self.graph.add((subj, self.seal, rdf.Literal["DOAJ Seal"])) 
           self.graph.add((subj, self.license, rdf.Literal["Journal license"])) 
           self.graph.add((subj, self.apc, rdf.Literal["APC"])) 
        return True       
   
    def pushDataToDb(self, path):
        store = rdf.SPARQLUpdateStore()
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        store.open((endpoint, endpoint))

        for triple in self.graph.triples((None, None, None)):
           store.add(triple)
        store.close()

class CategoryUploadHandler(UploadHandler):
    pass

class QueryHandler(Handler):
    pass

class JournalQueryHandler(QueryHandler):
    pass

class CategoryQueryHandler(QueryHandler):
    pass

