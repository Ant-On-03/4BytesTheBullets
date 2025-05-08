from Entities import Journal, Category, Area

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
            areas_df = c_queryHandler.getAllCategories()
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['journal_category_id'], row['category_id'], row['quartile']])
                    allCategories.append(category)
        return allCategories

    def getCategoriesWithQuartile(self, quartiles):
        pass

    def getCategoriesAssignedToAreas(self, area_ids):
        pass

    def getAreasAssignedToCategories(self, category_ids):
        pass














class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery, categoryQuery):
        super().__init__(journalQuery, categoryQuery)

    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        pass

    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        pass