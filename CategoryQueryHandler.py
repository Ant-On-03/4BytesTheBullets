

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

        
        # return all the areas in a database, with no repetitions.
        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()

        query = "SELECT issn, eissn,  FROM areas;"

        cursor.execute(query)
        


        joruanls = cursor.fetchall()
        df = pd.DataFrame(journals, columns=["journal_id"])

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

        # We use said query on the database.
        cursor.execute(query, tuple(area_ids))
        
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


