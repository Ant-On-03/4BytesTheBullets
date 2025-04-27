import rdflib as rdf
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
#Open in terminal: java -server -Xmx1g -jar blazegraph.jar
from sparql_dataframe import get

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

    def getLastUsedIndexFromMaster(self, master_csv_path):
        try:
            df_master = pd.read_csv(master_csv_path)
            if 'internal_id' in df_master.columns:
                internal_id = df_master['journal_id'].str.extract(r'journal-(\d+)')[0].dropna().astype(int)
                return internal_id.max() if not internal_id.empty else -1
            else:
                return -1
        except FileNotFoundError:
            return -1
    
    def cleanData(self, new_data_df):
        #Make column names shorter for readability 
        new_data_df = new_data_df.rename(columns={'Journal ISSN (print version)':'Journal ISSN', 
                                'Journal EISSN (online version)':'Journal EISSN',
                                'Languages in which the journal accepts manuscripts':'Languages'})
        #change null to ''
        new_data_df = new_data_df.fillna('')

        #drop any row with absolutely no id 
        new_data_df = new_data_df[((new_data_df['Journal ISSN'] != '') | (new_data_df['Journal EISSN'] != ''))]

        print('Data cleaned')
        return new_data_df
    
    def addTriples(self, new_data_df, start_idx=0):  
        new_ids = []
        for i, (_, row) in enumerate(new_data_df.iterrows(), start=start_idx):
            local_id = f"journal-{i}"
            new_ids.append(local_id)
            subj = rdf.URIRef(self.Journal + local_id)
            self.graph.add((subj, self.id_issn, rdf.Literal(row['Journal ISSN'])))  
            self.graph.add((subj, self.id_eissn, rdf.Literal(row["Journal EISSN"])))
            self.graph.add((subj, self.title, rdf.Literal(row["Journal title"])))
            self.graph.add((subj, self.language, rdf.Literal(row['Languages'])))
            self.graph.add((subj, self.publisher, rdf.Literal(row["Publisher"])))
            self.graph.add((subj, self.seal, rdf.Literal(row["DOAJ Seal"])))
            self.graph.add((subj, self.license, rdf.Literal(row["Journal license"])))
            self.graph.add((subj, self.apc, rdf.Literal(row["APC"])))
        
        new_data_df['internal_id'] = new_ids  # Track assigned IDs
        print('Graph populated')

        return new_data_df      
   
    def pushDataToDb(self, new_data_path, master_csv_path):
        new_data_df = pd.read_csv(new_data_path)
        store = SPARQLUpdateStore()
        #Open in terminal: java -server -Xmx1g -jar blazegraph.jar

        doaj_graph_uri = rdf.URIRef('https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph')

        # Get the last used journal_id from the master CSV
        last_index = self.getLastUsedIndexFromMaster(master_csv_path)

        #clean data
        new_data_df = self.cleanData(new_data_df)

        # Add triples and assign journal_ids
        df_with_ids = self.addTriples(new_data_df, start_idx=last_index + 1)

        #Open triple store 
        store.open((self.dbPathOrUrl, self.dbPathOrUrl))

        #Add triples to triple store using Dataset 
        #Dataset stores a collection (dataset) of the graphs in a triplestore, the default graph and any name graph 
        triplestore_ds = rdf.Dataset(store=store)
        #With the graph_uri store a specific named graph (context)
        doaj_graph = triplestore_ds.get_context(doaj_graph_uri)

        for triple in self.graph.triples((None, None, None)):
           doaj_graph.add(triple)
        triplestore_ds.close()

        print('Triples added to triplestore')

        #Append new data to master CSV file
        try:
            master_df = pd.read_csv(master_csv_path)
            updated_master_df = pd.concat([master_df, df_with_ids], ignore_index=True)
        except FileNotFoundError:
            updated_master_df = df_with_ids

        updated_master_df.to_csv(master_csv_path, index=False)
        print('Master CSV updated with new journals')

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

    def getById(self, id):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}
        {{ {self.fixed_where}
            FILTER (?issn = "{id}" || ?eissn = "{id}")
        }}
        }}     
        """
        journal_byId_df = get(self.dbPathOrUrl, query, True)

        return journal_byId_df
    
    def getAllJournals(self):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}
        {{ {self.fixed_where}
        }}
        }}     
        """
        journals_df = get(self.dbPathOrUrl, query, True)
        
        return journals_df

    def getJournalsWithTitle(self, partialTitle):
        # Escape double quotes in partialTitle to avoid breaking the SPARQL query
        safe_partialTitle = partialTitle.replace('"', '\\"')

        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}
        {{ {self.fixed_where}
            FILTER (CONTAINS(LCASE(str(?title)), LCASE("{safe_partialTitle}")))
        }}
        }}     
        """

        journals_byTitle_df = get(self.dbPathOrUrl, query, True)
            
        return journals_byTitle_df


    def getJournalsPublishedBy(self, partialName):
        # Escape double quotes in partialName to avoid breaking the SPARQL query
        safe_partialName = partialName.replace('"', '\\"')

        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}
        {{ {self.fixed_where}
            FILTER (CONTAINS(LCASE(str(?publisher)), LCASE("{safe_partialName}")))
        }}
        }}     
        """

        journals_byPub_df = get(self.dbPathOrUrl, query, True)
        
        return journals_byPub_df

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
        journals_withLicense_df = get(self.dbPathOrUrl, query, True)
        
        return journals_withLicense_df

    def getJournalsWithAPC(self):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}  
        {{ {self.fixed_where}
            FILTER (?apc = "Yes")   
        }}
        }}     
        """
        journals_withAPC_df = get(self.dbPathOrUrl, query, True)
        
        return journals_withAPC_df

    def getJournalsWithDOAJSeal(self):
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {self.fixed_graph}  
        {{ {self.fixed_where}
            FILTER (?seal = "Yes")   
        }}
        }}     
        """
        journals_withSeal_df = get(self.dbPathOrUrl, query, True)
        return journals_withSeal_df

class CategoryQueryHandler(QueryHandler):
    pass
