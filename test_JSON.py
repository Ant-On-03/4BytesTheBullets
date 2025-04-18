



# how to make dataframes from json file

import pandas as pd
import json


filePath = "./resources/scimago.json"



df = pd.read_json(filePath)

jsontest = df[:5].copy()

# add indentifyers columns to the dataframe

#just like this we have the 
journals = jsontest["identifiers"]

## ------------------------------------------- JOURNAL DATAFRAME ------------------------------------------- ##

# We chose as the primary key the firs id (if its not None) or the second one (if the first is None)
# This is to avoid creating duplicate primary keys in the database when we supplementing the database with an additional dataset
jsontest["unique_id"] = ["Journal-"+ row[0][0] if row[0][0] != None else "Journal-"+ row[0][1] for idx, row in jsontest.iterrows()]

################ERROR:#################
# For line above
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
################ERROR:#################

# We create a new dataframe with the unique_id and the identifiers columns
journals_df = jsontest[["unique_id"]]
# we separate the ISSN and EISSN because the journals graph database sometimes does not have one of them.
journals_df[['ISSN', 'EISSN']] = pd.DataFrame(jsontest['identifiers'].tolist(), index=jsontest.index)

################ERROR:#################
# For line above
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
################ERROR:#################

## ------------------------------------------- AREAS DATAFRAME ------------------------------------------- ##


# Get the Series 'areas' from the dataframe
areas_series = jsontest["areas"]
# Use explode to spread multiple values in the areas columns across different rows
areas_series = areas_series.explode("areas")
unique_areas = areas_series.drop_duplicates()
# Convert this back to a dataframe to be added to the database
area_df = pd.DataFrame(unique_areas, columns=["areas"])

print(area_df)

## ------------------------------------------- AREAS_JOURNALS DATAFRAME ------------------------------------------- ##

# We create a dataframe with the PRIMARY KEY for JOURNALS and for AREAS
areas_journals_dataframe = jsontest[['unique_id', 'areas']]
# we SEPARATE the AREAS since there are many areas for each JOURNAL in the table.
areas_journals_dataframe = areas_journals_dataframe.explode("areas")

## ------------------------------------------- CATEGORIES DATAFRAME ------------------------------------------- ##

# we create a table with ALL the DICTIONATIES of the CATEGORIES
dummy_dataframe = jsontest[['categories']].explode("categories")
# we normalize the dataframe into a flat table
categories_dataframe = pd.json_normalize(dummy_dataframe['categories'])

# print(categories_dataframe)

## ------------------------------------------- JOURNALS_CATEGORIES DATAFRAME ------------------------------------------- ##

# Take the unique identifiers from the journals dataframe
journals_categories_dataframe = pd.DataFrame(journals_df['unique_id'])


journals_categories_dataframe.insert(1, "categories", jsontest['categories'].values)

journals_categories_dataframe = journals_categories_dataframe.explode("categories")

# Extract id and quartile from the categories dictionary
journals_categories_dataframe['id'] = journals_categories_dataframe['categories'].apply(lambda x: x['id'])
journals_categories_dataframe['quartile'] = journals_categories_dataframe['categories'].apply(lambda x: x['quartile'])

# Drop the original categories column
journals_categories_dataframe = journals_categories_dataframe.drop('categories', axis=1)

# print(journals_categories_dataframe)

