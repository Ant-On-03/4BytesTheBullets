
import pandas as pd
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

            CREATE TABLE Journal_Categories (
                journal_id INT,
                category_id INT,
                PRIMARY KEY (journal_id, category_id),
                FOREIGN KEY (journal_id) REFERENCES Journals(journal_id),
                FOREIGN KEY (category_id) REFERENCES Categories(category_id)
            );

            CREATE TABLE Journal_Areas (
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
        # this method is to load the json file into a dataframe that we will adjust using the method addTables
        # and push to the database
        df = pd.read_json(path)
        # we need to see whether we want to clean our data
        self.cleanData(df)
        # this is where the dataframe created from the json file is added to our database 
        # according to the method we formed addTables, specifying how many there should be
        # and what names etc.
        self.addTables(df)
        
        return True 

class QueryHandler(Handler):
    pass

class JournalQueryHandler(QueryHandler):
    pass

class CategoryQueryHandler(QueryHandler):
    pass
