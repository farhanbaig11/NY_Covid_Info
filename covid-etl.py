import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from datetime import datetime

url = 'https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD'
content = requests.get(url).content
data = json.loads(content)


df = pd.DataFrame(data["data"])
df["load_date"] = datetime.now()
df["county"] = df[9].str.replace(" ", "").str.replace(".", "")
df.drop(range(0, 8), axis=1, inplace=True)
df.columns = ['test_date', 'countyname',
              'new_positives', 'cumulative_number_of_positives', 'total_number_of_tests', 'cumulative_number_of_tests', 'load_date', 'county']
df.drop('countyname', axis=1, inplace=True)

countyNames = df["county"].unique().tolist()

engine = create_engine('sqlite://', echo=False)

for row in countyNames:
    data = df.loc[df["county"] == row]
    data.to_sql(row, con=engine)

print(engine.execute("SELECT * FROM Albany").fetchall())
