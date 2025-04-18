
import pandas as pd
import json
from sqlite3 import connect

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

class CategoryUploadHandler(UploadHandler):

    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        self.createTables()
        
    def createTables(self):
        # Creating the dataframes to be added to the database
        df = pd.read_json(self.dbPathOrUrl)

        ## --------------------------------------------------- JOURNAL DATAFRAME ------------------------------------------------------- ##

        # We chose as the primary key the firs id (if its not None) or the second one (if the first is None)
        # This is to avoid creating duplicate primary keys in the database when we supplementing the database with an additional dataset
        df["unique_id"] = ["Journal-"+ row[0][0] if row[0][0] != None else "Journal-"+ row[0][1] for idx, row in df.iterrows()]

        # We create a new dataframe with the unique_id and the identifiers columns
        journals_df = df[["unique_id"]]
        # we separate the ISSN and EISSN because the journals graph database sometimes does not have one of them.
        journals_df[['ISSN', 'EISSN']] = pd.DataFrame(df['identifiers'].tolist(), index=df.index)

        ## --------------------------------------------------- AREAS DATAFRAME --------------------------------------------------------- ##

        # Get the Series 'areas' from the dataframe
        areas_series = df["areas"]
        # Use explode to spread multiple values in the areas columns across different rows
        areas_series = areas_series.explode("areas")
        unique_areas = areas_series.drop_duplicates()
        # Convert this back to a dataframe to be added to the database
        area_df = pd.DataFrame(unique_areas, columns=["areas"])

        ## ---------------------------------------------- AREAS_JOURNALS DATAFRAME ---------------------------------------------------- ##

        # We create a dataframe with the PRIMARY KEY for JOURNALS and for AREAS
        areas_journals_dataframe = df[['unique_id', 'areas']]
        # we SEPARATE the AREAS since there are many areas for each JOURNAL in the table.
        areas_journals_dataframe = areas_journals_dataframe.explode("areas")

        ## ------------------------------------------------ CATEGORIES DATAFRAME ------------------------------------------------------ ##

        # we create a table with ALL the DICTIONATIES of the CATEGORIES
        categories_dummy = df[['categories']].explode("categories")
        # we normalize the dataframe into a flat table
        categories_dataframe = pd.json_normalize(categories_dummy['categories'])

        ## ------------------------------------------- JOURNALS_CATEGORIES DATAFRAME ------------------------------------------- ##

        # Take the uniquely created identifiers from the journals dataframe
        journals_categories_dataframe = pd.DataFrame(journals_df['unique_id'])
        # Add the column from the original dataframe that still contains the list of dictionaries of 'ids' and 'quartiles'
        journals_categories_dataframe.insert(1, "categories", df['categories'].values)
        # Explode the list of dictionaries across multiple rows, so they correspond to the right journal as well
        journals_categories_dataframe = journals_categories_dataframe.explode("categories")

        # Extract id and quartile from each categories dictionary and use .apply() to add it as a string value to two new Series 
        # 'id' and 'quartile' in the journals_category_dataframe
        journals_categories_dataframe['id'] = journals_categories_dataframe['categories'].apply(lambda x: x['id'])
        journals_categories_dataframe['quartile'] = journals_categories_dataframe['categories'].apply(lambda x: x['quartile'])

        # Drop the original categories column that still holds the dictionaries
        journals_categories_dataframe = journals_categories_dataframe.drop('categories', axis=1)


        ## ------------------------------------------- POPULATING THE DATABASE ------------------------------------------- ##
        # Creating a relational database to host the tables
        connectionToDb =  connect(self.dbPathOrUrl)
        cursorToDb = connectionToDb.cursor()

        # Creating the tables
        with cursorToDb:
            # Creating the tables
            cursorToDb.execute("""
                               
            CREATE TABLE journals (
                journal_id PRIMARY KEY,
                issn UNIQUE
            );

            CREATE TABLE categories (
                category_id,
                category_name UNIQUE,
                quartile
            );

            CREATE TABLE areas (
                area_id INT AUTO_INCREMENT PRIMARY KEY,
                area_name VARCHAR(255) UNIQUE
            );

            CREATE TABLE journal_Categories (
                journal_id INT,
                category_id INT,
                PRIMARY KEY (journal_id, category_id),
                FOREIGN KEY (journal_id) REFERENCES Journals(journal_id),
                FOREIGN KEY (category_id) REFERENCES Categories(category_id)
            );

            CREATE TABLE journal_Areas (
                journal_id INT,
                area_id INT,
                PRIMARY KEY (journal_id, area_id),
                FOREIGN KEY (journal_id) REFERENCES Journals(journal_id),
                FOREIGN KEY (area_id) REFERENCES Areas(area_id)
            );

                           """)

    def pushDataToDb(self, filePath):
        # this method is to load the json file into a dataframe that we will adjust using the method addTables
        # and push to the database
        df = pd.read_json(filePath)
        # we need to see whether we want to clean our data
        self.cleanData(df)
        # this is where the dataframe created from the json file is added to our database 
        # according to the method we formed addTables, specifying how many there should be
        # and what names etc.
        self.addTables(df)


        # we turn the json into python iterable dictionaries


        #for journal in journals:

            # save the journal id

            # save the areas
        #    for area in areas:
                # save the areas

        
        return True
    
    def pushDataToDb(self, path):
        
        self.cleanData(df)
       
        self.addTables(df)
        
        return True 

class QueryHandler(Handler):
    pass

class JournalQueryHandler(QueryHandler):
    pass

class CategoryQueryHandler(QueryHandler):
    pass
