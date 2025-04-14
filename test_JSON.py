



# how to make dataframes from json file

import pandas as pd
import json


filePath = "./resources/scimago.json"



df = pd.read_json(filePath)

json = df[40:45]

print(json)


# add indentifyers columns to the dataframe

#just like this we have the 
journals = json["identifiers"]

## ------------------------------------------- JOURNAL DATAFRAME ------------------------------------------- ##

# We chose as the primary key the firs id (if its not None) or the second one (if the first is None)
# This is to avoid creating duplicate primary keys in the database when we supplementing the database with an additional dataset
json["unique_id"] = ["Journal-"+ row[0][0] if row[0][0] != None else "Journal-"+ row[0][1] for idx, row in json.iterrows()]

# We create a new dataframe with the unique_id and the identifiers columns
journals_df = json[["unique_id"]]
# we separate the ISSN and EISSN because the journals graph database sometimes does not have one of them.
journals_df[['ISSN', 'EISSN']] = pd.DataFrame(json['identifiers'].tolist(), index=json.index)

#print(journals_df)
#print(json)

## ------------------------------------------- AREAS DATAFRAME ------------------------------------------- ##


areas_series = json[["areas"]]
areas_series = areas_series.explode("areas")
unique_areas = areas_series.drop_duplicates()


# print(unique_areas)
# eliminate duplicates
#json = json.drop_duplicates(subset=['unique_id'])

## ------------------------------------------- AREAS_JOURNALS DATAFRAME ------------------------------------------- ##

# We create a dataframe with the PRIMARY KEY for JOURNALS and for AREAS
areas_journals_dataframe = json[['unique_id', 'areas']]
# we SEPARATE the AREAS since there are many areas for each JOURNAL in the table.
areas_journals_dataframe = areas_journals_dataframe.explode("areas")

## ------------------------------------------- CATEGORIES DATAFRAME ------------------------------------------- ##

# we create a table with ALL the DICTIONATIES of the CATEGORIES
dummy_dataframe = json[['categories']].explode("categories")
# we normalize the dataframe into a flat table
categories_dataframe = pd.json_normalize(dummy_dataframe['categories'])


## ------------------------------------------- JOURNALS_CATEGORIES DATAFRAME ------------------------------------------- ##

print(json)
journals_categories_dataframe = json[['unique_id', 'categories']]
journals_categories_dataframe = journals_categories_dataframe.explode("categories")
journals_categories_dataframe = pd.json_normalize(journals_categories_dataframe)

print(journals_categories_dataframe)

