# to do

1. Pagination



@. Snippets
conn = sqlite3.connect('./../data/databases/kielce_2025_07_03_09.db') 
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cur.fetchall()

for table in tables:
    print(table)


    # import pandas as pd
# import sqlite3

# # Load database in slq
# conn = sqlite3.connect("./../data/databases/kielce_2025_07_03_12.db")
# cur = conn.cursor()

# cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
# tables = cur.fetchall()

# for table in tables:
#     print(table)

# # SQL database to DataFrame
# dataframe = pd.read_sql("select * from apartment_data", conn)

# # Data overview
# print(dataframe.head())
# print(dataframe.shape)
# print(dataframe.isnull().sum())
# print(dataframe.describe(include="all"))

# dataframe=dataframe.drop_duplicates()
# print(dataframe.columns)
# dataframe=dataframe.drop(columns=["Unnamed: 0.1",'Unnamed: 0', 'ID',  'Title',])
# print(dataframe.columns)