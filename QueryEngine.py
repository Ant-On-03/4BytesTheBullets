from Entities import Journal, Category, Area
from Handlers import JournalQueryHandler, CategoryQueryHandler
import re



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
        if re.fullmatch(r"(?=.*\d)[\dX]{4}-[\dX]{4}", id):
            for j_queryHandler in self.journalQuery:
                journalWithId_df = j_queryHandler.getById(id)
                if not journalWithId_df.empty:
                    for _, row in journalWithId_df.iterrows():
                        journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                else:
                    journal = None

            if journal != None:
                for c_queryHandler in self.categoryQuery:
                    categoryWithId_df = c_queryHandler.getById(id)
                    if not categoryWithId_df.empty:
                        cat_l = []
                        area_l = []
                        for _, row in categoryWithId_df.iterrows():
                            category = Category([row['journal_id'], row['category_id']], row['quartile'])
                            area = Area(row['area_id'])
                            cat_l.append(category)
                            area_l.append(area)
                        journal.setCategories(cat_l)
                        journal.setAreas(area_l)
                            
                    else:
                        category = None
                        area = None
            
            return journal
            
        else:
            for c_queryHandler in self.categoryQuery:
                categoryWithId_df = c_queryHandler.getById(id)
                if not categoryWithId_df.empty:
                    if id in 'category_id':
                        category = Category([id])
                    elif id in 'area_id':
                        area = Area([id])

                else:
                    category = None
                    area = None

            if category == None and area == None:
                return None
            
            elif category != None:
                return category
            
            else:
                return area

    def getAllJournals(self) -> list[Journal]:
        alljournals = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getAllJournals()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    alljournals.append(journal)
        return alljournals

    def getJournalsWithTitle(self, partialTitle) -> list[Journal]:
        journalsWithTitle = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithTitle(partialTitle)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithTitle.append(journal)

        return journalsWithTitle

    def getJournalsPublishedBy(self, partialName) -> list[Journal]:
        journalsPublishedBy = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsPublishedBy(partialName)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsPublishedBy.append(journal)
        return journalsPublishedBy

    def getJournalsWithLicense(self, licenses) -> list[Journal]:
        journalsWithLicense = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsPublishedBy(licenses)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithLicense.append(journal)
        return journalsWithLicense

    def getJournalsWithAPC(self) -> list[Journal]:
        journalsWithAPC = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithAPC()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithAPC.append(journal)
        return journalsWithAPC

    def getJournalsWithDOAJSeal(self) -> list[Journal]:
        journalsWithDOAJSeal = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithDOAJSeal()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    journal = Journal([row['issn'],row['eissn']], row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithDOAJSeal.append(journal)
        return journalsWithDOAJSeal

    def getAllCategories(self) -> list[Journal]:
        allCategories = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getAllCategories()
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['journal_id'], row['category_id']], row['quartile'])
                    allCategories.append(category)
        return allCategories

    def getAllAreas(self) -> list[Journal]:
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
        journals = []
        if len(category_ids) == 0 and len(quartiles) == 0:
            return self.getAllJournals()

        elif len(category_ids) == 0:
            catWithQ = self.getCategoriesWithQuartile(quartiles)
            for cat in catWithQ:
                j = self.getEntityById(cat.getIds[0])
                journals.append(j)

        elif len(quartiles) == 0:
            categories = self.getAllCategories()
            for cat in categories:
                j = self.getEntityById(cat.getIds[0])
                journals.append(j)

        else:
            catWithQ = self.getCategoriesWithQuartile(quartiles)
            for cat in catWithQ:
                if cat.getIds()[1] in category_ids:
                    j = self.getEntityById(cat.getIds[0])
                    journals.append(j)

        return journals


    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        journals = []
        if len(areas_ids) == 0 and len(licenses) == 0:
            return self.getAllJournals()
        
        elif len(areas_ids) == 0:
            return self.getJournalsWithLicense(licenses)
        
        elif len(licenses) == 0:
            for j in self.getAllJournals():
                for a in areas:
                    if a.getIds()[0] in j.getIds():
                        j.setAreas(list(a))
                        journals.append(j)
        else:
            jWithLicenses = self.getJournalsWithLicense(licenses)
            areas = []
            for area in self.getAllAreas:
                if area.getIds()[1] in areas_ids:
                    areas.append(area)
            
            for j in jWithLicenses:
                for a in areas:
                    if a.getIds()[0] in j.getIds():
                        j.setAreas(list(a))
                        journals.append(j)
            
            return journals

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        journals = []
        journalsWithAPC_l = self.getJournalsWithAPC()
        
        if len(areas_ids) == 0 and len(category_ids) == 0 and len(quartiles) == 0:
            return journalsWithAPC_l
        
        else:
            jInCatWithQ_l = self.getJournalsInCategoriesWithQuartile(category_ids, quartiles)

            #find all specified areas
            areas = []
            for area in self.getAllAreas:
                if area.getIds()[1] in areas_ids:
                    areas.append(area)

            #find all journals with APC in specified areas
            jInAreas_l = []
            for j in journalsWithAPC_l:
                for a in areas:
                    if a.getIds()[0] in j.getIds():
                        j.setAreas(list(a))
                        jInAreas_l.append(j)

            #find all journals with APC in specified areas in category with quartile
            for j in jInAreas_l:
                for jCat in jInCatWithQ_l:
                    if j.getIds() == jCat.getIds():
                        journals.append(j)

            return journals
            
            
