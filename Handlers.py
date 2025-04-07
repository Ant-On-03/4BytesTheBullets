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
      
       #add URI for journal class
       self.Journal = "https://schema.org/Periodical"
       #add URI for journal attributes
       self.id_issn = rdf.URIRef("https://schema.org/identifier/issn")
       self.id_eissn = rdf.URIRef("https://schema.org/identifier/eissn")
       self.title = rdf.URIRef("https://schema.org/name")
       self.language = rdf.URIRef("https://schema.org/language")
       self.publisher = rdf.URIRef("https://schema.org/publisher")
       self.seal = rdf.URIRef("https://schema.org/award")
       self.license = rdf.URIRef("https://schema.org/license")
       self.apc = rdf.URIRef("https://schema.org/apc")

       super().__init__(dbPathOrUrl)

    def cleanData(self,df):
        #Make column names shorter for readability 
        df = df.rename(columns={'Journal ISSN (print version)':'Journal ISSN', 
                                  'Journal EISSN (online version)':'Journal EISSN',
                                  'Languages in which the journal accepts manuscripts':'Languages'})
        #change null to ''
        df = df.fillna('')

        #drop any row with absolutely no id 
        df = df[df['Journal ISSN'] != '' & df['Journal EISSN']!= '']
        
        return True
    
    def addTriples(self,df):       
        for idx, row in df.iterrow():
           local_id = "journal-" + str(idx)
           subj = rdf.URIRef(self.Journal + local_id)
           self.graph.add((subj, self.id_issn, rdf.Literal(row['Journal ISSN'])))  
           self.graph.add((subj, self.id_eissn, rdf.Literal(row["Journal EISSN"])))
           self.graph.add((subj, self.title, rdf.Literal(row["Journal title"])))

           #Make seperate triples for each language
           #self.graph.add((subj, self.language, rdf.Literal(row['Languages'])))
           for lan in row['Languages'].split(', '):
               self.graph.add((subj, self.language, rdf.Literal(lan)))

           self.graph.add((subj, self.publisher, rdf.Literal(row["Publisher"])))
           self.graph.add((subj, self.seal, rdf.Literal(row["DOAJ Seal"])))
           self.graph.add((subj, self.license, rdf.Literal(row["Journal license"])))
           self.graph.add((subj, self.apc, rdf.Literal(row["APC"])))
        
        return True       
   
    def pushDataToDb(self, path):
        df = pd.read_csv(path)
        store = rdf.SPARQLUpdateStore()

        self.cleanData(df)
        self.addTriples(df)

        store.open((self.dbPathOrUrl, self.dbPathOrUrl))

        for triple in self.graph.triples((None, None, None)):
           store.add(triple)
        store.close()

        return True

class CategoryUploadHandler(UploadHandler):
    pass

class QueryHandler(Handler):
    pass

class JournalQueryHandler(QueryHandler):
    pass

class CategoryQueryHandler(QueryHandler):
    pass

