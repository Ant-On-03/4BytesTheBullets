

### QUERY_HANDLER ###

from Handlers import *
import pandas as pd
from sqlite3 import connect
from pandas import DataFrame

class QueryHandler(Handler):

    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def getById(self, id):
        pass
    





class CategoryQueryHandler(QueryHandler):

    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def getById(self, id:str) -> DataFrame:

        
        # RETURN A JOIN OF ALL THE TABLES THAT HAVE THE ID OF THE JOURNAL.
        # This is the query that will be used to get the journal with the id given.
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()

        query = """
        
        
        SELECT DISTINCT j.issn, j.eissn, jc.category_id, cq.quartile, aj.area_id 
        FROM journals AS j
        JOIN journals_categories AS jc ON j.journal_id = jc.journal_id
        JOIN categories_quartiles AS cq ON jc.category_id = cq.category_id
        JOIN areas_journals AS aj ON j.journal_id = aj.journal_id
        WHERE (j.issn = ? OR j.eissn = ?);
    
        
        """

        cursor.execute(query, (id, id))

        print("query:", query)


        journals = cursor.fetchall()
        df = pd.DataFrame(journals)

        conn.close()
        
        return df

        


    def getAllCategories(self) -> DataFrame:

        # return all the categories in a database, with no repetitions.

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT category_id FROM categories;")
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

        conn.close()
    
        return df



    def getAllAreas(self) -> DataFrame:

        # return all the areas in a database, with no repetitions.
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT area_id FROM areas;")
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["area_id"])

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
                    SELECT DISTINCT category_id 
                    FROM categories_quartiles 
                    WHERE quartile IN ({

                        ', '.join(['?'] * len(quartiles)) 

                    })
                    """ # This willl create as many "?" placeholders as the number of quartiles asked for to execute in the query.

        print("query:", query)

        # We use said query on the database.
        cursor.execute(query, tuple(quartiles))
        
        # We turn it into a Dataframe
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

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

                    SELECT DISTINCT category_id

                    FROM journals_categories AS jc
                    JOIN journals AS j ON jc.journal_id = j.journal_id
                    JOIN areas_journals AS aj ON j.journal_id = aj.journal_id

                    WHERE aj.area_id IN ({

                        ', '.join(['?'] * len(area_ids)) 

                    })

                    """ 
        print(query)

        # We use said query on the database.
        cursor.execute(query, tuple(area_ids))

        print("cursor.fetchall():", cursor.fetchall())
        
        # We turn it into a Dataframe
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

        conn.close()

        return df



    def getAreasAssignedToCategories(self, categories:set[str] ) -> DataFrame:

        if len(categories) == 0:
            # If no area is given, we return all the categories.
            return self.getAllAreas()

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()


        query =  f"""

                    SELECT DISTINCT area_id

                    FROM journals_categories AS jc
                    JOIN journals AS j ON jc.journal_id = j.journal_id
                    JOIN areas_journals AS aj ON j.journal_id = aj.journal_id

                    WHERE jc.category_id IN ({

                        ', '.join(['?'] * len(categories)) 

                    })

                    """ 

        # We use said query on the database.
        cursor.execute(query, tuple(categories))
        
        # We turn it into a Dataframe
        areas = cursor.fetchall()
        df = pd.DataFrame(areas, columns=["area_id"])

        conn.close()

        return df
    
        
    


if __name__ == "__main__":
    
    CategoryUploadHandler = CategoryUploadHandler("a.db")
    CategoryUploadHandler.pushDataToDb("./resources/scimago.json")
    CategoryQueryHandler = CategoryQueryHandler("a.db")


    areas = {}
    categories = CategoryQueryHandler.getCategoriesAssignedToAreas(areas)
    print("CATEGORIES ASSIGNED TO AREAS")
    print("length OF THEM:", len(categories))
    print(categories)

    categories = {"Catalysis","Philosophy"}
    areas = CategoryQueryHandler.getAreasAssignedToCategories(categories)
    print("AREAS ASSIGNED TO CATEGORIES")
    print("length OF THEM:", len(areas))
    print(areas)




    getById = CategoryQueryHandler.getById("19")
    print("GET BY ID")
    print("length OF THEM:", len(getById))
    # LETS PRINT ALL ROWS IN THE DATAFRAME.
    pd.set_option('display.max_rows', None)
    print(getById)


from Entities import Journal, Category, Area

class BasicQueryEngine(object):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        self.journalQuery = journalQuery
        self.categoryQuery = categoryQuery
        
    def getAllCategories(self):
            allCategories = []
            for c_queryHandler in self.categoryQuery:
                categories_df = c_queryHandler.getAllCategories()
                if not categories_df.empty:
                    for _, row in categories_df.iterrows():
                        category = Category([row['journal_category_id'], row['category_id'], row['quartile']])
                        allCategories.append(category)
            return allCategories