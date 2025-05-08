
import pandas as pd
import json
from sqlite3 import connect
import os

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
        print(journals_df[40:50])

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
        res = ""
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM categories;")
        for row in cursor.fetchall():
            #res + str(row)
            print(row)
        conn.close()
        
        return res
            




class QueryHandler(Handler):
    pass

class JournalQueryHandler(QueryHandler):
    pass

class CategoryQueryHandler(QueryHandler):
    pass




if __name__ == "__main__":


    CategoryUploadHandler = CategoryUploadHandler("a.db")

    CategoryUploadHandler.pushDataToDb("./resources/scimago.json")

    print("Data pushed to database")
    print("CATEGORIES IN THE DATABASE")
    # print(CategoryUploadHandler.__str__())