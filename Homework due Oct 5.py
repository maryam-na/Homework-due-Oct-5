# This script gets Chicago Crime Data from Google Public Datasets and performs some operations on the data.
from google.cloud import bigquery
import matplotlib.pyplot as plt
import pandas as pd


class CrimeDataAnalyzer:
    def __init__(self, credentials, input_query):
        self.credentials = credentials
        self.client = bigquery.Client.from_service_account_json(self.credentials)
        self.input_query = input_query
# This method imports the data from Google public datasets
    def import_data(self):
        query_job = self.client.query(self.input_query)  # Start Query API Request
        query_result = query_job.result()  # Get Query Result
        df = query_result.to_dataframe()  # Save the Query Result to Dataframe
        return df
# This method outputs a bar plot from the received data from the query
    def plot_data(self, data_frame):
        data_frame.plot.bar(x='primary_type', rot=90)
        plt.show()

# This method returns the maximum percentage change of primary crimes between 2015 to 2016
    def max_pct_change(self, data_frame):
        data_frame['pct_change_2015_to_2016'] = pd.to_numeric(data_frame['pct_change_2015_to_2016'])
        return data_frame['pct_change_2015_to_2016'].max()

# This method return the minimum percentage change of primary crimes between 2015 to 2016
    def min_pct_change(self, data_frame):
        data_frame['pct_change_2015_to_2016'] = pd.to_numeric(data_frame['pct_change_2015_to_2016'])
        return data_frame['pct_change_2015_to_2016'].min()


query = (
    "SELECT "
    "primary_type, description, "
    "COUNTIF(year = 2015) AS arrests_2015, "
    "COUNTIF(year = 2016) AS arrests_2016, "
    "FORMAT('%3.2f',(COUNTIF(year = 2016) - COUNTIF(year = 2015)) / COUNTIF(year = 2015)*100) AS pct_change_2015_to_2016 "
    "FROM "
    "`bigquery-public-data.chicago_crime.crime` "
    "WHERE arrest = TRUE AND year IN (2015, 2016) "
    "GROUP BY primary_type, description "
    "HAVING COUNTIF(year = 2015) > 100 ORDER BY (COUNTIF(year = 2016) - COUNTIF(year = 2015)) / COUNTIF(year = 2015) DESC LIMIT 10"
)

crimeDataAnalyzer = CrimeDataAnalyzer('credentials.json', query)
data = crimeDataAnalyzer.import_data()
max_pct=crimeDataAnalyzer.max_pct_change(data)
min_pct=crimeDataAnalyzer.min_pct_change(data)
print(f"The maximum and minimum percentage change of primary crimes between 2015 to 2016 are {max_pct} and {min_pct}, respectively.")
crimeDataAnalyzer.plot_data(data)
