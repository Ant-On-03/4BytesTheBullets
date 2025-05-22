# Importing all the classes for handling the relational database
from Handlers import CategoryUploadHandler, CategoryQueryHandler, JournalUploadHandler, JournalQueryHandler

# Importing the class for dealing with mashup queries
from QueryEngine import FullQueryEngine

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
cat = CategoryUploadHandler(rel_path)
cat.pushDataToDb("resources/scimago.json")
print("relational database created")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# Then, create the graph database (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://10.5.0.2:9999/blazegraph/sparql"
jou = JournalUploadHandler(grp_endpoint)
jou.pushDataToDb("resources/test_doaj.csv", "master.csv")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# In the next passage, create the query handlers for both
# the databases, using the related classes
cat_qh = CategoryQueryHandler(rel_path)

jou_qh = JournalQueryHandler(grp_endpoint)

# Finally, create a advanced mashup object for asking
# about data
que = FullQueryEngine()
que.addCategoryHandler(cat_qh)
que.addJournalHandler(jou_qh)

result_q1 = que.getAllJournals()
result_q2 = que.getAllCategories()
result_q3 = que.getAllAreas()
result_q4 = que.getEntityById('2198-9761')
result_q5 = que.getEntityById("Artificial Intelligence")
result_q6 = que.getJournalsInCategoriesWithQuartile({"Artificial Intelligence", "Oncology"}, {"Q1"})
result_q7 = que.getJournalsInAreasWithLicense({"Agricultural and Biological Sciences", "Biochemistry, Genetics and Molecular Biology"}, {"CC BY", "CC BY-NC-ND"})
result_q8 = que.getDiamondJournalsInAreasAndCategoriesWithQuartile({"Medicine", "Nursing"},{"Medicine (miscellaneous)", "Nutrition and Dietetics"},{"Q4"})

print(result_q8)

