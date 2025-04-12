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

class CategoryUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl):
        # Creating a relational database to host the tables

        self.reldatabase = ...

        super().__init__(dbPathOrUrl)

    def addTables(self, df):
        # here is where we add tables to the dataframe
        return True
    
    def cleanData(self, df):
        # the option to clean our data here
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
