from Entities import Journal, Category, Area
from Handlers import JournalQueryHandler, CategoryQueryHandler, CategoryUploadHandler, JournalUploadHandler
import re
import pandas as pd


class BasicQueryEngine(object):
    """
    BasicQueryEngine is a class that provides methods to query and manipulate journal and category data.
    It allows for adding and cleaning query handlers, retrieving journals and categories based on various criteria,
    and getting entities by their IDs.
    
    Attributes:
        journalQuery (list): A list of journal query handlers.
        categoryQuery (list): A list of category query handlers.
        
    Methods:
        cleanJournalHandlers(): Cleans the journal query handlers.
        cleanCategoryHandlers(): Cleans the category query handlers.
        addJournalHandler(handler): Adds a journal query handler to the list.
        addCategoryHandler(handler): Adds a category query handler to the list.
        getEntityById(id): Retrieves a journal or category entity by its ID.
        getAllJournals(): Retrieves all journal entities.
        getJournalsWithTitle(partialTitle): Retrieves journals with a title that contains the specified partial title.
        getJournalsPublishedBy(partialName): Retrieves journals published by a publisher with a name that contains the specified partial name.
        getJournalsWithLicense(licenses): Retrieves journals with a specific license.
        getJournalsWithAPC(): Retrieves journals with an Article Processing Charge (APC).
        getJournalsWithDOAJSeal(): Retrieves journals with a DOAJ seal.
        getAllCategories(): Retrieves all category entities.
        getAllAreas(): Retrieves all area entities.
        getCategoriesWithQuartile(quartiles): Retrieves categories with a specific quartile.
        getCategoriesAssignedToAreas(area_ids): Retrieves categories assigned to specific areas.
        getAreasAssignedToCategories(category_ids): Retrieves areas assigned to specific categories.

    """
    def __init__(self, journalQuery=None, categoryQuery=None):
        self.journalQuery = journalQuery if journalQuery is not None else []
        self.categoryQuery = categoryQuery if categoryQuery is not None else []

    def cleanJournalHandlers(self):
        """
        Cleans the journal query handlers by resetting the journalQuery list.
        """
        self.journalQuery = []
        return True

    def cleanCategoryHandlers(self):
        """
        Cleans the category query handlers by resetting the categoryQuery list.
        """
        self.categoryQuery = []
        return True

    def addJournalHandler(self, handler):
        """
        Adds a journal query handler to the list of journal query handlers.

        Args:
            handler (JournalQueryHandler): The journal query handler to be added.
        
        Returns:
            bool: True if the handler was added successfully, False otherwise.
        
        """
        self.journalQuery.append(handler)
        return True

    def addCategoryHandler(self, handler):
        """
        Adds a category query handler to the list of category query handlers.
        
        Args:
            handler (CategoryQueryHandler): The category query handler to be added.
        
        Returns:
            bool: True if the handler was added successfully, False otherwise.
        
        """
        self.categoryQuery.append(handler)
        return True

    def getEntityById(self, id):
        """
        Retrieves a journal or category or area entity by its ID.
        If the ID matches the pattern for a journal, it retrieves the journal and its categories and areas.

        Args:
            id (str): The ID of the journal or category to be retrieved.
        
        Returns:
            identity (Journal or Category or Area): The journal or category entity corresponding to the ID.
        """

        if re.fullmatch(r"(?=.*\d)[\dX]{4}-[\dX]{4}", id):
            found_journal = False
            journal = None

            for j_queryHandler in self.journalQuery:
                journalWithId_df = j_queryHandler.getById(id)
                if not journalWithId_df.empty:

                    for _, row in journalWithId_df.iterrows():
                        id = []
                        if pd.notna(row['issn']) == True:
                            id.append(row['issn'])
                        if pd.notna(row['eissn']) == True:
                            id.append(row['eissn'])

                        found_journal = True
                        journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                
            if found_journal:
                for c_queryHandler in self.categoryQuery:
                    categoryWithId_df = c_queryHandler.getById(id)
                    if not categoryWithId_df.empty:
                        cat_l = []
                        area_l = []
                        for _, row in categoryWithId_df.iterrows():
                            category = Category([row['category_id']])
                            area = Area(row['area_id'])
                            cat_l.append(category)
                            area_l.append(area)
                        journal.setCategories(cat_l)
                        journal.setAreas(area_l)
                            
                    else:
                        category = None
                        area = None
            
            return journal

        
        category = None
        area = None
        
        for c_queryHandler in self.categoryQuery:
            categoryWithId_df = c_queryHandler.getById(id)
            if not categoryWithId_df.empty:
                if id in categoryWithId_df['category_id'].values:
                    category = Category([id])
                    return category
                
                elif id in categoryWithId_df['area_id'].values:
                    area = Area([id])
                    return area

                # else:
                #     category = None
                #     area = None

            return None
            
    def getAllJournals(self) -> list[Journal]:
        """
        Retrieves all journal entities from the journal query handlers.

        Args:
            None
        
        Returns:
            list[Journal]: A list of all journal entities.
        """

        alljournals = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getAllJournals()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    alljournals.append(journal)
        return alljournals

    def getJournalsWithTitle(self, partialTitle) -> list[Journal]:

        journalsWithTitle = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithTitle(partialTitle)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithTitle.append(journal)

        return journalsWithTitle

    def getJournalsPublishedBy(self, partialName) -> list[Journal]:
        journalsPublishedBy = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsPublishedBy(partialName)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsPublishedBy.append(journal)
        return journalsPublishedBy

    def getJournalsWithLicense(self, licenses) -> list[Journal]:
        journalsWithLicense = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithLicense(licenses)
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithLicense.append(journal)
        return journalsWithLicense

    def getJournalsWithAPC(self) -> list[Journal]:
        journalsWithAPC = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithAPC()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithAPC.append(journal)
        return journalsWithAPC

    def getJournalsWithDOAJSeal(self) -> list[Journal]:
        journalsWithDOAJSeal = []
        for j_queryHandler in self.journalQuery:
            journals_df = j_queryHandler.getJournalsWithDOAJSeal()
            if not journals_df.empty:
                for _, row in journals_df.iterrows():
                    id = []
                    if pd.notna(row['issn']) == True:
                        id.append(row['issn'])
                    if pd.notna(row['eissn']) == True:
                        id.append(row['eissn'])
                    journal = Journal(id, row['title'], row['language'], row['seal'], row['license'], row['apc'], row['publisher'])
                    journalsWithDOAJSeal.append(journal)
        return journalsWithDOAJSeal

    def getAllCategories(self) -> list[Category]:
        """
        Retrieves all category entities from the category query handlers.
        
        Args:
            None
        
        Returns:
            list[Category]: A list of all category entities.
        """
        allCategories = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getAllCategories()
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['category_id']], row['journal_quartile_dict'])
                    allCategories.append(category)
        return allCategories

    def getAllAreas(self) -> list[Area]:
        """
        Retrieves all area entities from the category query handlers.

        Args:
            None

        Returns:
            list[Area]: A list of all area entities.
        """
        allAreas = []
        for a_queryHandler in self.categoryQuery:
            areas_df = a_queryHandler.getAllAreas()
            if not areas_df.empty:
                for _, row in areas_df.iterrows():
                    areas = Area([row['area_id']], row['journal'])
                    allAreas.append(areas)
        return allAreas

    def getCategoriesWithQuartile(self, quartiles) -> list[Category]:
        """
        Retrieves categories with a specific quartile from the category query handlers.

        Args:
            quartiles (set): A set of quartiles to filter categories by.

        Returns:
            list[Category]: A list of categories with the specified quartile.
        """

        categoriesWithQuartile = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getCategoriesWithQuartile(quartiles)
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['category_id']], row['journal_quartile_dict'])
                    categoriesWithQuartile.append(category)
        return categoriesWithQuartile

    def getCategoriesAssignedToAreas(self, area_ids) -> list[Category]:
        """
        Retrieves categories assigned to specific areas from the category query handlers.

        Args:
            area_ids (set): A set of area IDs to filter categories by.

        Returns:
            list[Category]: A list of categories assigned to the specified areas.
        
        """
        categoriesAssignedToAreas = []
        for c_queryHandler in self.categoryQuery:
            categories_df = c_queryHandler.getCategoriesAssignedToAreas(area_ids)
            if not categories_df.empty:
                for _, row in categories_df.iterrows():
                    category = Category([row['category_id']], row['journal_quartile_dict'])
                    categoriesAssignedToAreas.append(category)
        return categoriesAssignedToAreas
    

    def getAreasAssignedToCategories(self, category_ids) -> list[Area]:
        """
        Retrieves areas assigned to specific categories from the category query handlers.
        
        Args:
            category_ids (set): A set of category IDs to filter areas by.
        
        Returns:
            list[Area]: A list of areas assigned to the specified categories.
        """
        areasAssignedToCategories = []
        for c_queryHandler in self.categoryQuery:
            areas_df = c_queryHandler.getAreasAssignedToCategories(category_ids)
            if not areas_df.empty:
                for _, row in areas_df.iterrows():
                    area = Area([row['area_id']], row['journal'])
                    areasAssignedToCategories.append(area)
        return areasAssignedToCategories


class FullQueryEngine(BasicQueryEngine):
    """
    FullQueryEngine is a subclass of BasicQueryEngine that provides additional methods to query and manipulate journal and category data.

    Attributes:
        journalQuery (list): A list of journal query handlers.
        categoryQuery (list): A list of category query handlers.

    Methods:
        __init__(journalQuery=None, categoryQuery=None): Initializes the FullQueryEngine with optional journal and category query handlers.
        getJournalsInCategoriesWithQuartile(category_ids, quartiles): Retrieves journals in specific categories with quartiles.
        getJournalsInAreasWithLicense(areas_ids, licenses): Retrieves journals in specific areas with licenses.
        getDiamondJournalsInAreasAndCategoriesWithQuartile(areas_ids, category_ids, quartiles): Retrieves diamond journals in areas and categories with quartiles.
    """

    def __init__(self, journalQuery=None, categoryQuery=None):
        super().__init__(journalQuery, categoryQuery)

    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles) -> list[Journal]:
        """
        Retrieves journals in specific categories with quartiles from the journal and category query handlers.
        
        Args:
            category_ids (set): A set of category IDs to filter journals by.
            quartiles (set): A set of quartiles to filter journals by.
        Returns:
            list[Journal]: A list of journals in the specified categories with the specified quartiles.
        """

        journals = []
        if len(category_ids) == 0 and len(quartiles) == 0:
            cat_l = self.getAllCategories()
            area_l = self.getAllAreas()
            for j in self.getAllJournals():
                journal_cat_l= []
                journal_area_l = []
                for c in cat_l:
                    if j.getIds()[0] in list(c.getJournalQuartile().keys()):
                        journal_cat_l.append(c) 
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals
        


        elif len(category_ids) == 0:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()
            for j in self.getAllJournals():
                journal_cat_l= []
                journal_area_l = []
                for c in catWithQ_l:
                    if j.getIds()[0] in c.getJournalQuartile().keys():
                        journal_cat_l.append(c) 
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals


        elif len(quartiles) == 0:
            cat_l = self.getAllCategories()
            area_l = self.getAllAreas()
            for j in self.getAllJournals():
                journal_cat_l= []
                journal_area_l = []
                for c in cat_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c) 
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        else:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()

            for j in self.getAllJournals():
                journal_cat_l= []
                journal_area_l = []
                for c in catWithQ_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c) 

                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals


    def getJournalsInAreasWithLicense(self, areas_ids, licenses) -> list[Journal]:
        """
        Retrieves journals in specific areas with licenses from the journal and category query handlers.

        Args:
            areas_ids (set): A set of area IDs to filter journals by.
            licenses (set): A set of licenses to filter journals by. 
        Returns:
            list[Journal]: A list of journals in the specified areas with the specified licenses.
        """
        journals = []
        if len(areas_ids) == 0 and len(licenses) == 0:
            return self.getAllJournals()
        
        elif len(areas_ids) == 0:
            return self.getJournalsWithLicense(licenses)
        
        elif len(licenses) == 0:
            # Iterate over all journals
            for j in self.getAllJournals():
                # Iterate over all areas
                for a in areas:
                    # Check if the journal that has this area is also in the list of journals
                    if a.getIds()[0] in j.getIds():
                        j.setAreas(list(a))
                        journals.append(j)
        else:
            jWithLicenses = self.getJournalsWithLicense(licenses)
            areas = []
            # We treat areas as a weak entity, getAllAreas are going to retrieve the areas that are assigned to the journals
            for area in self.getAllAreas():
                # Check if the area is in the specified areas (areas_ids)
                if area.getIds()[1] in areas_ids:
                    # Create a list to store those areas to be used later for the intersection license x areas
                    areas.append(area)
            
            # Now we pick the intersection between the journals with licenses and the areas
            # We "pick" those journals that are in both lists (that one with all journals in the areas and the one with all the journals with the licenses)
            for j in jWithLicenses:
                for a in areas:
                    if a.getIds()[0] in j.getIds():
                        j.setAreas([a])
                        journals.append(j)
            
            return journals

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles) -> list[Journal]:
        """
        Retrieves diamond journals in specific areas and categories with quartiles from the journal and category query handlers.
        Args:
            areas_ids (set): A set of area IDs to filter journals by.
            category_ids (set): A set of category IDs to filter journals by.
            quartiles (set): A set of quartiles to filter journals by.
        Returns:
            list[Journal]: A list of diamond journals in the specified areas and categories with the specified quartiles.
        """
        # First get all diamond journals (journals without APC)
        allJournals = self.getAllJournals()
        diamondJournals = [j for j in allJournals if not j.hasAPC()]
        journals = []

        # Case 1: No filters specified (all areas, categories, and quartiles)
        if len(areas_ids) == 0 and len(category_ids) == 0 and len(quartiles) == 0:
            cat_l = self.getAllCategories()
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in cat_l:
                    if j.getIds()[0] in c.getJournalQuartile().keys():
                        journal_cat_l.append(c)
                
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 2: Only quartiles specified
        elif len(areas_ids) == 0 and len(category_ids) == 0:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in catWithQ_l:
                    if j.getIds()[0] in c.getJournalQuartile().keys():
                        journal_cat_l.append(c)
                
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 3: Only categories specified
        elif len(areas_ids) == 0 and len(quartiles) == 0:
            # Get areas assigned to the specified categories
            area_l = self.getAreasAssignedToCategories(category_ids)
            cat_l = self.getAllCategories()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                # Get categories for this journal
                for c in cat_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c)
                
                # Get areas for this journal
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 4: Only areas specified
        elif len(category_ids) == 0 and len(quartiles) == 0:
            # Get categories assigned to the specified areas
            cat_l = self.getCategoriesAssignedToAreas(areas_ids)
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                # Get categories for this journal
                for c in cat_l:
                    if j.getIds()[0] in c.getJournalQuartile().keys():
                        journal_cat_l.append(c)
                
                # Get areas for this journal
                for a in area_l:
                    if a.getIds()[0] in areas_ids:
                        if j.getIds()[0] in a.getJournal():
                            journal_area_l.append(a)
                
                if journal_cat_l and journal_area_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 5: Categories and quartiles specified
        elif len(areas_ids) == 0:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in catWithQ_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c)
                
                for a in area_l:
                    if j.getIds()[0] in a.getJournal():
                        journal_area_l.append(a)
                
                if journal_cat_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 6: Areas and categories specified
        elif len(quartiles) == 0:
            cat_l = self.getAllCategories()
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in cat_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c)
                
                for a in area_l:
                    if a.getIds()[0] in areas_ids:
                        if j.getIds()[0] in a.getJournal():
                            journal_area_l.append(a)
                
                if journal_cat_l and journal_area_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 7: Areas and quartiles specified
        elif len(category_ids) == 0:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in catWithQ_l:
                    if j.getIds()[0] in c.getJournalQuartile().keys():
                        journal_cat_l.append(c)
                
                for a in area_l:
                    if a.getIds()[0] in areas_ids:
                        if j.getIds()[0] in a.getJournal():
                            journal_area_l.append(a)
                
                if journal_cat_l and journal_area_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

        # Case 8: All filters specified
        else:
            catWithQ_l = self.getCategoriesWithQuartile(quartiles)
            area_l = self.getAllAreas()
            
            for j in diamondJournals:
                journal_cat_l = []
                journal_area_l = []
                
                for c in catWithQ_l:
                    if c.getIds()[0] in category_ids:
                        if j.getIds()[0] in c.getJournalQuartile().keys():
                            journal_cat_l.append(c)
                
                for a in area_l:
                    if a.getIds()[0] in areas_ids:
                        if j.getIds()[0] in a.getJournal():
                            journal_area_l.append(a)
                
                if journal_cat_l and journal_area_l:
                    j.setCategories(journal_cat_l)
                    j.setAreas(journal_area_l)
                    journals.append(j)
            return journals

                
        # else:

        #     if len(category_ids) == 0 or len(quartiles) == 0:
        #         jInCatWithQ_l = self.getAllCategories()
                
        #     else:
        #         jInCatWithQ_l = self.getJournalsInCategoriesWithQuartile(category_ids, quartiles)

        #     #find all specified areas

        #     if len(areas_ids) == 0:
        #         areas = self.getAllAreas()

        #     else:
        #         areas = []
        #         for area in self.getAllAreas():
        #             if area.getIds()[1] in areas_ids:
        #                 areas.append(area)



        #     #find all journals with APC in specified areas
        #     jInAreas_l = []
        #     for j in diamondJournals:
        #         for a in areas:
        #             if a.getIds()[0] in j.getIds():
        #                 j.setAreas([a])
        #                 jInAreas_l.append(j)

        #     #find all journals with APC in specified areas in category with quartile

        #     for j in jInAreas_l:
        #         for jCat in jInCatWithQ_l:
        #             if j.getIds() == jCat.getIds():
        #                 journals.append(j)

        #     return journals
            
            
if __name__ == "__main__":
    # Example usage
    # UploadedJournalHandler = JournalQueryHandler("ppespp.db")
    UploadHandler = CategoryUploadHandler("ttestt.db")
    UploadHandler.pushDataToDb("./resources/scimago.json")

    QueryHandler = CategoryQueryHandler("ttestt.db")

    # Initialize the query engines
    # These handlers should be populated with actual data retrieval logic


    basic_query_engine = BasicQueryEngine([], [QueryHandler])
    # full_query_engine = FullQueryEngine()

    print(basic_query_engine.getAreasAssignedToCategories(["Artificial Intelligence"]))


    # Add handlers and perform queries as needed
    # Example: basic_query_engine.addJournalHandler(JournalQueryHandler())
    # Example: journals = full_query_engine.getJournalsInCategoriesWithQuartile(['cat1', 'cat2'], ['Q1', 'Q2'])
    pass