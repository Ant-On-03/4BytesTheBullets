



# how to make dataframes from json file

import pandas as pd
import json


filePath = "./resources/scimago.json"



df = pd.read_json(filePath)




jsontest = df.copy()

# add indentifyers columns to the dataframe

#just like this we have the 

 # Creating the dataframes to be added to the database
df = pd.read_json(filePath)

## ------------------------------------------- JOURNAL DATAFRAME ------------------------------------------- ##

# We chose as the primary key the firs id (if its not None) or the second one (if the first is None)
# This is to avoid creating duplicate primary keys in the database when we supplementing the database with an additional dataset
df["journal_id"] = ["Journal-"+ row[0][0] if row[0][0] != None else "Journal-"+ row[0][1] for idx, row in df.iterrows()]

### WE DROP THE DUPLICATES
df = df.drop_duplicates(subset=["journal_id"])


# We create a new dataframe with the unique_id and the identifiers columns
journals_df = df[["journal_id"]]
# we separate the ISSN and EISSN because the journals graph database sometimes does not have one of them.
journals_df[['ISSN', 'EISSN']] = pd.DataFrame(df['identifiers'].tolist(), index=df.index)



## ------------------------------------------- AREAS DATAFRAME ------------------------------------------- ##


# Get the Series 'areas' from the dataframe
#areas_series = df["areas"]
# Use explode to spread multiple values in the areas columns across different rows
#areas_series = areas_series.explode("areas")
#unique_areas = areas_series.drop_duplicates()
# Convert this back to a dataframe to be added to the database
#area_df = pd.DataFrame(unique_areas, columns=["areas"])
#area_df.rename(columns={"areas": "area_id"}, inplace=True)

## ------------------------------------------- AREAS DATAFRAME ------------------------------------------- ##

# We create a dataframe with the PRIMARY KEY for JOURNALS and for AREAS
areas_dataframe = df[['journal_id', 'areas']]
# we SEPARATE the AREAS since there are many areas for each JOURNAL in the table.
areas_dataframe = areas_dataframe.explode("areas")
areas_dataframe.rename(columns={"areas": "area_id"}, inplace=True)
print("AREAS DATAFRAME")

## ------------------------------------------- CATEGORIES_QUARTILES DATAFRAME ------------------------------------------- ##

# we create a table with ALL the DICTIONATIES of the CATEGORIES
#dummy_dataframe = df[['categories']].explode("categories")
# we normalize the dataframe into a flat table
#categories_quartiles_dataframe = pd.json_normalize(dummy_dataframe['categories'])
#categories_quartiles_dataframe.rename(columns={"id": "category_id"}, inplace=True)

## DROP DUPLICATES
#categories_quartiles_dataframe = categories_quartiles_dataframe.drop_duplicates(subset=["category_id","quartile"])

## ------------------------------------------- CATEGORIES DATAFRAME ------------------------------------------- ##


## DROP DUPLICATES
#categories_dataframe = categories_quartiles_dataframe["category_id"].copy()
#categories_dataframe = categories_dataframe.drop_duplicates()
#categories_dataframe = pd.DataFrame(categories_dataframe, columns = ["category_id"])

# categories_dataframe = categories_dataframe.drop_duplicates(subset=["category_id"])


## ------------------------------------------- CATEGORIES DATAFRAME ------------------------------------------- ##

# Take the unique identifiers from the journals dataframe
categories_dataframe = pd.DataFrame(journals_df['journal_id'])
categories_dataframe.insert(1, "categories", df['categories'].values)

categories_dataframe = categories_dataframe.explode("categories")
# Extract id and quartile from the categories dictionary
categories_dataframe['category_id'] = categories_dataframe['categories'].apply(lambda x: x['id'])
categories_dataframe['quartile'] = categories_dataframe['categories'].apply(lambda x: x.get('quartile'))
# Drop the original categories column
categories_dataframe = categories_dataframe.drop('categories', axis=1)



print("LENGTH OF CATEGORIES DATAFRAME", len(categories_dataframe))








# COUNT THE NUMBER OF ROWS IN THE DATAFRAME
#print(len(jsontest["journal_id"]))



print("LENGTH DF")
print(len(df))
print("END")



