import rdflib as rdf
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
#Open in terminal: java -server -Xmx1g -jar blazegraph.jar

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
       self.graph=rdf.Graph(identifier=rdf.URIRef('https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph'))
      
       #add URI for journal class
       self.Journal = "https://schema.org/Periodical"
       #add URI for journal attributes
       self.id_issn = rdf.URIRef("https://schema.org/issn")
       self.id_eissn = rdf.URIRef("https://schema.org/eissn")
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
        df = df[((df['Journal ISSN'] != '') | (df['Journal EISSN'] != ''))]

        print('Data cleaned')
        return df
    
    def addTriples(self,df):  
        for idx, row in df.iterrows():
           local_id = "journal-" + str(idx)
           subj = rdf.URIRef(self.Journal + local_id)
           self.graph.add((subj, self.id_issn, rdf.Literal(row['Journal ISSN'])))  
           self.graph.add((subj, self.id_eissn, rdf.Literal(row["Journal EISSN"])))
           self.graph.add((subj, self.title, rdf.Literal(row["Journal title"])))

           #Make seperate triples for each language
           self.graph.add((subj, self.language, rdf.Literal(row['Languages'])))

           self.graph.add((subj, self.publisher, rdf.Literal(row["Publisher"])))
           self.graph.add((subj, self.seal, rdf.Literal(row["DOAJ Seal"])))
           self.graph.add((subj, self.license, rdf.Literal(row["Journal license"])))
           self.graph.add((subj, self.apc, rdf.Literal(row["APC"])))
        
        print('Graph populated')
        return True       
   
    def pushDataToDb(self, path):
        df = pd.read_csv(path)
        store = SPARQLUpdateStore()
        #Open in terminal: java -server -Xmx1g -jar blazegraph.jar

        doaj_graph_uri = rdf.URIRef('https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph')

        self.addTriples(self.cleanData(df))

        #Open triple store 
        store.open((self.dbPathOrUrl, self.dbPathOrUrl))

        #Add triples to triple store using ConjunctiveGraph 
        #ConjunctiveGraph stores a collection (dataset) of the graphs in a triplestore, the default graph and any name graph 
        triplestore_ds = rdf.ConjunctiveGraph(store=store)
        #With the graph_uri store a specific named graph (context)
        doaj_graph = triplestore_ds.get_context(doaj_graph_uri)

        for triple in self.graph.triples((None, None, None)):
           doaj_graph.add(triple)
        triplestore_ds.close()

        print('Triples added to triplestore')
        return True

    
class CategoryUploadHandler(UploadHandler):
    pass

class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id):
        pass

class JournalQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        self.fixed_schema_select = "PREFIX schema:<https://schema.org/> SELECT ?journal ?issn ?eissn ?title ?publisher ?language ?license ?seal ?apc"
        self.fixed_graph = "GRAPH <https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph>"
        self.fixed_where = "?journal schema:issn ?issn; schema:eissn ?eissn; schema:name ?title; schema:publisher ?publisher; schema:language ?language; schema:license ?license; schema:award ?seal; schema:apc ?apc ."

    def getAllJournals(self):
        pass

    def getJournalsWithTitle(self, partialTitle):
        pass

    def getJournalsPublishedBy(self, partialName):
        pass

    def getJournalsWithLicense(self, licenses):
        import itertools
        def query_generator(licenses):
        
        # create a list from the set to generate all combination patterns
            terms = list(licenses)
        
        # create all patterns of combinations
            patterns  = list(itertools.permutations(terms))

        # convert patterns to string 
            pattern_to_string = [", ".join(p) for p in patterns]
        
        # create a regex expression for exact match
            regex = "^(" + "|".join(pattern_to_string) + ")$"

        # create a SPARQL FILTER with regex
            regex_filter = f'FILTER(REGEX(?license, "{regex}"))'
            return regex_filter

    # run function and store result
        regex_query = query_generator(licenses)
    
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}
        {{ {self.fixed_where}
            {regex_query} 
        }}
        }}     
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        print(f"Jounals with license of {licenses}: ", df_sparql)
        return True

    def getJournalsWithAPC(self):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}  
        {{ {self.fixed_where}
            FILTER (?apc = "Yes")   
        }}
        }}     
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        print("Jounals with APC: ", df_sparql)
        return df_sparql

    def getJournalsWithDOAJSeal(self):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}  
        {{ {self.fixed_where}
            FILTER (?seal = "Yes")   
        }}
        }}     
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        print("Jounals with DOAJ Seal: ", df_sparql)
        return df_sparql

class CategoryQueryHandler(QueryHandler):
    pass
