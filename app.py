from CrimeDataAnalyzer import CrimeDataAnalyzer

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
job = crimeDataAnalyzer.client.list_jobs()
data = crimeDataAnalyzer.import_data()
max_pct = crimeDataAnalyzer.max_pct_change(data)
min_pct = crimeDataAnalyzer.min_pct_change(data)
print(
    f"The maximum and minimum percentage change of primary crimes between 2015 to 2016 are {max_pct} and {min_pct}, respectively.")
crimeDataAnalyzer.plot_data(data)
