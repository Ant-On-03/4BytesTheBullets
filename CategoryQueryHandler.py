

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
        pass


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

        


    def getCategoriesWithQuartile(self, quartile:set[str]) -> DataFrame:

        conn = connect(self.dbPathOrUrl)
        cursor = conn.cursor()
        cursor.execute("SELECT category_id FROM categories_quartiles WHERE quartile = ?", (quartile,))
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

        conn.close()
    
        return df
        
        categories = cursor.fetchall()
        df = pd.DataFrame(categories, columns=["category_id"])

        conn.close()
    
        pass

    def getCategoriesAssignedToAreas(self, area_ids:set[str] ) -> DataFrame:
    
        pass

    def getAreasAssignedToCategories(self, categories:set[str] ) -> DataFrame:
    
        pass
    


if __name__ == "__main__":
    

    CategoryUploadHandler = CategoryUploadHandler("a.db")

    CategoryUploadHandler.pushDataToDb("./resources/scimago.json")


    CategoryQueryHandler = CategoryQueryHandler("a.db")
    categories = CategoryQueryHandler.getAllCategories()
    print("ALL CATEGORIES")
    print("length OF THEM:", len(categories))
    print(categories)


    areas = CategoryQueryHandler.getAllAreas()
    print(areas)


    quartile = "Q4"
    print(f"Categories with quartile {quartile}:")
    caregoriesWithQuartile = CategoryQueryHandler.getCategoriesWithQuartile(quartile)
    print(caregoriesWithQuartile)
