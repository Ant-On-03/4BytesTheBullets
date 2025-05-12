#libraries for graph database
import rdflib as rdf
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
#Open in terminal: java -server -Xmx1g -jar blazegraph.jar
from sparql_dataframe import get

#libraries for relational database
import pandas as pd
from sqlite3 import connect
import os
from pandas import DataFrame
import re

class Handler(object):
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl
        return True

## ------------------------------------------- UPLOAD HANDLERS ------------------------------------------- ## 

class UploadHandler(Handler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def pushDataToDb(self, path):
        pass

class JournalUploadHandler(UploadHandler): #Shiho and Regina
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
        new_data_df = pd.read_csv(new_data_path, delimiter= ";" )
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

    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        self.createTables()
        
    def createTables(self):
        ## ------------------------------------------- CREATING THE DATABASE ------------------------------------------- ##
        # Creating a relational database to host the tables

        if os.path.exists(self.dbPathOrUrl):
            os.remove(self.dbPathOrUrl)

        connectionToDb =  connect(self.dbPathOrUrl)
        cursorToDb = connectionToDb.cursor()

        # Creating the tables

        cursorToDb.execute("""
                            
        CREATE TABLE journals (
            journal_id TEXT PRIMARY KEY,
            ISSN TEXT,
            EISSN TEXT
        );""")

        cursorToDb.execute("""
                            
        CREATE TABLE areas (
            area_id TEXT PRIMARY KEY
        );
        """)

        # FOR US CATEGORIES IS A WEAK ENTITY, THAT DEPENDS ON THE JOURNAL ID.
        cursorToDb.execute("""
                            
        CREATE TABLE categories (
            journal_id TEXT,
            category_id TEXT,
            quartile TEXT,
            PRIMARY KEY (journal_id, category_id),
            FOREIGN KEY (journal_id) REFERENCES journals(journal_id)
        );

        """)
        cursorToDb.execute("""
                            
        CREATE TABLE areas_journals (
            journal_id TEXT,
            area_id TEXT,
            PRIMARY KEY (journal_id, area_id),
            FOREIGN KEY (journal_id) REFERENCES journals(journal_id),
            FOREIGN KEY (area_id) REFERENCES areas(area_id)
        );

                        """) 

        # Commit the changes and close the connection
        connectionToDb.commit()
        connectionToDb.close()

        return True
        
    def pushDataToDb(self, filePath):

        # Creating the dataframes to be added to the database
        df = pd.read_json(filePath)
    
        ## ------------------------------------------- JOURNAL DATAFRAME ------------------------------------------- ##

        # We chose as the primary key the firs id (if its not None) or the second one (if the first is None)
        # This is to avoid creating duplicate primary keys in the database when we supplementing the database with an additional dataset
        df["journal_id"] = [row[0][0] if row[0][0] != None else row[0][1] for idx, row in df.iterrows()]

        ### WE DROP THE DUPLICATES
        df = df.drop_duplicates(subset=["journal_id"])


        # We create a new dataframe with the unique_id and the identifiers columns
        journals_df = df[["journal_id"]]
        # we separate the ISSN and EISSN because the journals graph database sometimes does not have one of them.
        journals_df[['ISSN', 'EISSN']] = pd.DataFrame(df['identifiers'].tolist(), index=df.index)

        ## ------------------------------------------- AREAS DATAFRAME ------------------------------------------- ##
        # Get the Series 'areas' from the dataframe
        areas_series = df["areas"]
        # Use explode to spread multiple values in the areas columns across different rows
        areas_series = areas_series.explode("areas")
        unique_areas = areas_series.drop_duplicates()
        # Convert this back to a dataframe to be added to the database
        area_df = pd.DataFrame(unique_areas, columns=["areas"])
        area_df.rename(columns={"areas": "area_id"}, inplace=True)

        ## ------------------------------------------- AREAS_JOURNALS DATAFRAME ------------------------------------------- ##

        # We create a dataframe with the PRIMARY KEY for JOURNALS and for AREAS
        areas_journals_dataframe = df[['journal_id', 'areas']]
        # we SEPARATE the AREAS since there are many areas for each JOURNAL in the table.
        areas_journals_dataframe = areas_journals_dataframe.explode("areas")
        areas_journals_dataframe.rename(columns={"areas": "area_id"}, inplace=True)

        ## ------------------------------------------- CATEGORIES DATAFRAME ------------------------------------------- ##

        # Take the unique identifiers from the journals dataframe
        categories_dataframe = pd.DataFrame(journals_df['journal_id'])
        categories_dataframe.insert(1, "categories", df['categories'].values)

        categories_dataframe = categories_dataframe.explode("categories")
        # Extract id and quartile from the categories dictionary
        categories_dataframe['category_id'] = categories_dataframe['categories'].apply(lambda x: x['id'])
        categories_dataframe['quartile'] = categories_dataframe['categories'].apply(lambda x: x.get('quartile'))
        # Drop the original categories column
        categories_dataframe = categories_dataframe.drop('categories', axis=1)


        ############## ----------------- INSERTING DATA INTO THE DATABASE ----------------- ##############
        # Creating the connection to the database
        connectionToDb = connect(self.dbPathOrUrl)
        cursorToDb = connectionToDb.cursor()

        # Iterating through the dataframes and inserting the data into the database
        for idx, row in journals_df.iterrows():
            cursorToDb.execute("""
                               INSERT INTO journals (journal_id, ISSN, EISSN) 
                               VALUES (?, ?, ?);
                               """, (row["journal_id"], row["ISSN"], row["EISSN"]))
            
        for idx, row in area_df.iterrows():
            cursorToDb.execute("""
                               INSERT INTO areas (area_id) 
                               VALUES (?);
                               """, (row["area_id"],))
        
        for idx, row in areas_journals_dataframe.iterrows():
            cursorToDb.execute("""
                               INSERT INTO areas_journals (journal_id, area_id) 
                               VALUES (?, ?);
                               """, (row["journal_id"], row["area_id"]))
        
        for idx, row in categories_dataframe.iterrows():
            cursorToDb.execute("""
                               INSERT INTO categories (journal_id, category_id, quartile)
                               VALUES (?, ?, ?);
                               """, (row["journal_id"], row["category_id"], row["quartile"]))
        
        # Commit the changes and close the connection
        connectionToDb.commit()
        connectionToDb.close()

        return True
    
    def __str__(self):

         # return all the categories in a database, with no repetitions.

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT category_id FROM categories;")
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

        conn.close()
    
        return df

## ------------------------------------------- QUERY HANDLERS ------------------------------------------- ## 

class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id):
        pass

class JournalQueryHandler(QueryHandler): #Shiho and Regina
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        self.fixed_schema_select = "PREFIX schema:<https://schema.org/> SELECT ?journal ?issn ?eissn ?title ?publisher ?language ?license ?seal ?apc"
        self.fixed_graph = "GRAPH <https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph>"
        self.fixed_where = "?journal schema:issn ?issn; schema:eissn ?eissn; schema:name ?title; schema:publisher ?publisher; schema:language ?language; schema:license ?license; schema:award ?seal; schema:apc ?apc ."

    # Check if the named graph exists in the triple store. 
    # If the named graph exists, it will be used in the query. Otherwise, the query will be executed without the graph clause = the default graph.
    # This method is called at the beginning of each query method for journals.
    def graph_exists(self):
        query = f"""
        SELECT ?s WHERE {{ GRAPH <https://github.com/Ant-On-03/4BytesTheBullets/DOAJGraph> {{ ?s ?p ?o }} }}
        LIMIT 1
        """
        response = get(self.dbPathOrUrl, query, True)
        return not response.empty

    def getById(self, id):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
        
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}
        {{ {self.fixed_where}
            FILTER (?issn = "{id}" || ?eissn = "{id}")
        }}
        }}     
        """
        journal_byId_df = get(self.dbPathOrUrl, query, True)

        return journal_byId_df
    
    def getAllJournals(self):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
        
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}
        {{ {self.fixed_where}
        }}
        }}     
        """
        journals_df = get(self.dbPathOrUrl, query, True)
        
        return journals_df

    def getJournalsWithTitle(self, partialTitle):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
        
        # Escape double quotes in partialTitle to avoid breaking the SPARQL query
        safe_partialTitle = partialTitle.replace('"', '\\"')

        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}
        {{ {self.fixed_where}
            FILTER (CONTAINS(LCASE(str(?title)), LCASE("{safe_partialTitle}")))
        }}
        }}     
        """

        journals_byTitle_df = get(self.dbPathOrUrl, query, True)
            
        return journals_byTitle_df


    def getJournalsPublishedBy(self, partialName):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
        
        # Escape double quotes in partialName to avoid breaking the SPARQL query
        safe_partialName = partialName.replace('"', '\\"')

        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}
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
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
    
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}
        {{ {self.fixed_where}
            {regex_query} 
        }}
        }}     
        """
        journals_withLicense_df = get(self.dbPathOrUrl, query, True)
        
        return journals_withLicense_df

    def getJournalsWithAPC(self):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""

        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}  
        {{ {self.fixed_where}
            FILTER (?apc = "Yes")   
        }}
        }}     
        """
        journals_withAPC_df = get(self.dbPathOrUrl, query, True)
        
        return journals_withAPC_df

    def getJournalsWithDOAJSeal(self):
        if self.graph_exists():
            graph_clause = self.fixed_graph
        else:
            graph_clause = ""
        query = f"""
        {self.fixed_schema_select}
        WHERE {{ {graph_clause}  
        {{ {self.fixed_where}
            FILTER (?seal = "Yes")   
        }}
        }}     
        """
        journals_withSeal_df = get(self.dbPathOrUrl, query, True)
        return journals_withSeal_df


class CategoryQueryHandler(QueryHandler): #Anton and Anouk

    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def getById(self, id:str) -> DataFrame:
        
        if re.fullmatch(r"(?=.*\d)[\dX]{4}-[\dX]{4}", id):
            # RETURN A JOIN OF ALL THE TABLES THAT HAVE THE ID OF THE JOURNAL.
            # This is the query that will be used to get the journal with the id given.
            conn = connect(self.dbPathOrUrl)
            cursor = conn.cursor()

            query = """
            
            
            SELECT DISTINCT j.issn, j.eissn, c.category_id, c.quartile, aj.area_id 
            FROM journals AS j
            JOIN categories AS c ON j.journal_id = c.journal_id
            JOIN areas_journals AS aj ON j.journal_id = aj.journal_id
            WHERE (j.issn = ? OR j.eissn = ?);
        
            
            """

            cursor.execute(query, (id, id))
            # print("query:", query)
            journals = cursor.fetchall()
            df = pd.DataFrame(journals, columns=["issn", "eissn", "category_id", "quartile", "area_id"])
            conn.close()
        else:
            
            conn = connect(self.dbPathOrUrl)
            cursor = conn.cursor()
            query = """
            
            
            SELECT DISTINCT category_id, aj.area_id 
            FROM categories AS c
            JOIN areas_journals AS aj ON c.journal_id = aj.journal_id
            WHERE (category_id = ? OR aj.area_id = ?);
        
            
            """
            cursor.execute(query, (id, id))
            
            cat_area = cursor.fetchall()
            df = pd.DataFrame(cat_area, columns=["category_id", "area_id"])

            conn.close()       
        return df

    def getAllCategories(self) -> DataFrame:

        # return all the categories in a database, with no repetitions.

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT journal_id, category_id, quartile FROM categories;")
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["journal_id", "category_id", "quartile"])

        conn.close()
    
        return df



    def getAllAreas(self) -> DataFrame:

        # return all the areas in a database, with no repetitions.
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT journal_id, area_id FROM areas_journals;")
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["journal_id", "area_id"])

        conn.close()
        
        return df

        


    def getCategoriesWithQuartile(self, quartiles:set[str]) -> DataFrame:

        if len(quartiles) == 0:
            # If no quartile is given, we return all the categories.
            return self.getAllCategories()
            
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()

        # WE CREATE SUCH QUERY THAT IT RETURNS WHAT IS NEEDED.
        # For that we put in intersect as many times as the number of quartiles -1.


        query =  f"""
                    SELECT DISTINCT journal_id, category_id, quartile
                    FROM categories
                    WHERE quartile IN ({

                        ', '.join(['?'] * len(quartiles)) 

                    })
                    """ # This willl create as many "?" placeholders as the number of quartiles asked for to execute in the query.

        # print("query:", query)

        # We use said query on the database.
        cursor.execute(query, tuple(quartiles))
        
        # We turn it into a Dataframe
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["journal_id", "category_id", "quartile"])
        conn.close()

        return df
        # This is in case what we wanted was the INTERSECTIO instead of the UNION.
        query = """
                    SELECT DISTINCT category_id 
                    FROM categories_quartiles 
                    WHERE quartile IN (?)
                    """
        for i in range(len(quartiles)-1):
            query += """
                    INTERSECT
                    SELECT DISTINCT category_id 
                    FROM categories_quartiles 
                    WHERE quartile IN (?)
                    """
        print("query:", query)
        # We use said query on the database.
        cursor.execute(query, tuple(quartiles))
        # We turn it into a Dataframe
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])
        conn.close()
        return df



    def getCategoriesAssignedToAreas(self, area_ids:set[str] ) -> DataFrame:
        if len(area_ids) == 0:
            # If no area is given, we return all the categories.
            return self.getAllCategories()

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()


        query =  f"""

                    SELECT DISTINCT jc.journal_id, jc.category_id, jc.quartile

                    FROM categories AS jc
                    JOIN journals AS j ON jc.journal_id = j.journal_id
                    JOIN areas_journals AS aj ON j.journal_id = aj.journal_id

                    WHERE aj.area_id IN ({

                        ', '.join(['?'] * len(area_ids)) 

                    })

                    """ 
        # print(query)

        # We use said query on the database.
        cursor.execute(query, tuple(area_ids))
        # We turn it into a Dataframe
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["journal_id", "category_id", "quartile"])
        conn.close()
        return df

    def getAreasAssignedToCategories(self, categories:set[str] ) -> DataFrame:

        if len(categories) == 0:
            # If no area is given, we return all the categories.
            return self.getAllAreas()
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        query =  f"""

                    SELECT DISTINCT aj.journal_id, aj.area_id

                    FROM categories AS c
                    JOIN journals AS j ON c.journal_id = j.journal_id
                    JOIN areas_journals AS aj ON j.journal_id = aj.journal_id

                    WHERE c.category_id IN ({

                        ', '.join(['?'] * len(categories)) 

                    })

                    """ 
        # We use said query on the database.
        cursor.execute(query, tuple(categories))
        # We turn it into a Dataframe
        areas = cursor.fetchall()
        df = pd.DataFrame(areas, columns=["journal_id", "area_id"])
        conn.close()
        return df
    
