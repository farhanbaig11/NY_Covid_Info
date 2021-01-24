import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from datetime import datetime

#Bring Data through URL provided
url = 'https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD'
content = requests.get(url).content
data = json.loads(content)

#Creating Dataframe through data extracted
df = pd.DataFrame(data["data"])

# Adding Today's date in Dataframe
df["load_date"] = datetime.now()

# Adding another County column, with removing spaces and period for better table naming "New York", "St. Lawrence"
df["county"] = df[9].str.replace(" ", "").str.replace(".", "")

#Droping extra columns from Dataframe
df.drop(range(0, 8), axis=1, inplace=True)

#Adding column names
df.columns = ['test_date', 'countyname',
              'new_positives', 'cumulative_number_of_positives', 'total_number_of_tests', 'cumulative_number_of_tests', 'load_date', 'county']

#Droping default countyname field as it is not required              
df.drop('countyname', axis=1, inplace=True)

#Making unique counties for table creation and loop
countyNames = df["county"].unique().tolist()

#Create engine for in-memory db
engine = create_engine('sqlite://', echo=False)

#Loop to create table and insert data into it
for row in countyNames:
    data = df.loc[df["county"] == row]
    data.to_sql(row, con=engine)

#Example of running data
print(engine.execute("SELECT * FROM Albany").fetchall())
