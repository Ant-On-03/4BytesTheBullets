from Entities import Journal, Category, Area
from Handlers import *


class BasicQueryEngine(object):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        self.journalQuery = journalQuery
        self.categoryQuery = categoryQuery

    def cleanJournalHandlers(self):
        self.journalQuery = []
        return True

    def cleanCategoryHandlers(self):
        self.categoryQuery = []
        return True

    def addJournalHandler(self, handler):
        self.journalQuery.append(handler)
        return True

    def addCategoryHandler(self, handler):
        self.categoryQuery.append(handler)
        return True

    def getEntityById(self, id):
        for j_queryHandler in self.journalQuery:
            journalWithId_df = j_queryHandler.getById(id)
            if not journalWithId_df.empty:
                for _, row in journalWithId_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
            else:
                journal = None
        return journal

    def getAllJournals(self):
        alljournals = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getAllJournals()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    alljournals.append(journal)
        return alljournals

    def getJournalsWithTitle(self, partialTitle):
        journalsWithTitle = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithTitle(partialTitle)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithTitle.append(journal)

        return journalsWithTitle

    def getJournalsPublishedBy(self, partialName):
        journalsPublishedBy = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsPublishedBy(partialName)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsPublishedBy.append(journal)
        return journalsPublishedBy

    def getJournalsWithLicense(self, licenses):
        journalsWithLicense = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsPublishedBy(licenses)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithLicense.append(journal)
        return journalsWithLicense

    def getJournalsWithAPC(self):
        journalsWithAPC = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithAPC()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithAPC.append(journal)
        return journalsWithAPC

    def getJournalsWithDOAJSeal(self):
        journalsWithDOAJSeal = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithDOAJSeal()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithDOAJSeal.append(journal)
        return journalsWithDOAJSeal

    def getAllCategories(self):
        allCategories = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getAllCategories()
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['journal_id'], row['category_id']], row['quartile'])
                    allCategories.append(category)
        return allCategories

    def getAllAreas(self):
        allAreas = []
        for a_queryHandler in self.categoryQuery:
            areas_df = a_queryHandler.getAllAreas()
            if not areas_df.empty:
                for _, row in areas_df.iterrows():
                    areas = Area([row['journal_id'], row['area_id']])
                    allAreas.append(areas)
        return allAreas

    def getCategoriesWithQuartile(self, quartiles):
        categoriesWithQuartile = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getCategoriesWithQuartile(quartiles)
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['journal_id'], row['category_id']], row['quartile'])
                    categoriesWithQuartile.append(category)
        return categoriesWithQuartile

    def getCategoriesAssignedToAreas(self, area_ids):
        categoriesAssignedToAreas = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getCategoriesAssignedToAreas(area_ids)
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['journal_id'], row['category_id']], row['quartile'])
                    categoriesAssignedToAreas.append(category)
        return categoriesAssignedToAreas
    

    def getAreasAssignedToCategories(self, category_ids):
        areasAssignedToCategories = []
        for c_queryHandler in self.categoryQuery:
            areas_df = c_queryHandler.getAreasAssignedToCategories(category_ids)
            if not areas_df.empty:
                for _, row in areas_df.iterrows():
                    area = Area([row['journal_id'], row['area_id']])
                    areasAssignedToCategories.append(area)
        return areasAssignedToCategories













class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery, categoryQuery):
        super().__init__(journalQuery, categoryQuery)

    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        pass

    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        pass




def test():
    return 0

if __name__ == "__main__":
    

    # Test the BasicQueryEngine class
    journalQuery = []  # Replace with actual query handlers
    categoryQuery = []  # Replace with actual query handlers
    query_engine = BasicQueryEngine(journalQuery, categoryQuery)

    # WE CREATE THE QUERYHANDLER

    # We need to create some mock handlers to test the methods
    # lets create a CategoryUploadHandler
    Cat_UploadHandlerd = CategoryUploadHandler("a.db")
    Cat_UploadHandlerd.pushDataToDb("./resources/scimago.json")
    Cat_QueryHandler = CategoryQueryHandler("a.db")

    ## WE ADD THE HANDLER TO THE QUERY ENGINE
    query_engine.addCategoryHandler(Cat_QueryHandler)


    categories = query_engine.getAreasAssignedToCategories({"Artificial Intelligence"})
    for category in categories:
        print(category.getIds())

    #areas = query_engine.getAllAreas()
    #for area in areas:
    #    print(area.getQuartile())
    test()