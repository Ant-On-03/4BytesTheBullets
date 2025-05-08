

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
        
        
        SELECT DISTINCT j.issn, j.eissn, c.category_id, c.quartile, aj.area_id 
        FROM journals AS j
        JOIN categories AS c ON j.journal_id = c.journal_id
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
                    SELECT DISTINCT category_id 
                    FROM categories
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

                    FROM categories AS jc
                    JOIN journals AS j ON jc.journal_id = j.journal_id
                    JOIN areas_journals AS aj ON j.journal_id = aj.journal_id

                    WHERE aj.area_id IN ({

                        ', '.join(['?'] * len(area_ids)) 

                    })

                    """ 
        print(query)

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
        df = pd.DataFrame(areas, columns=["area_id"])

        conn.close()

        return df
    
        
    
def test():

    UploadHandler = CategoryUploadHandler("a.db")
    UploadHandler.pushDataToDb("./resources/scimago.json")
    QueryHandler = CategoryQueryHandler("a.db")

    areas = QueryHandler.getAllAreas()
    print("All areas:", areas)

    categories=QueryHandler.getAllCategories()
    print("All categories:", categories)

    categories = QueryHandler.getCategoriesWithQuartile({"Q1"})
    print("Categories with quartile Q1 and Q2:", categories)

    categories = QueryHandler.getAreasAssignedToCategories({"Drug Discovery"})
    print("Areas assigned to categorie", categories)

    areas = QueryHandler.getCategoriesAssignedToAreas({"Medicine"})
    print("Categories assigned to area", areas)

    IDs = QueryHandler.getById("2058-7546")
    print("IDs:", IDs)


if __name__ == "__main__":
    
    test()