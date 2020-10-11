import pytest
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

def test_data_not_empty():
    data = crimeDataAnalyzer.import_data()
    assert not data.empty

def test_import_data():
    data = crimeDataAnalyzer.import_data()
    columns = data.columns
    assert columns[0] == 'primary_type'
    assert columns[1] == 'description'
    assert columns[2] == 'arrests_2015'
    assert columns[3] == 'arrests_2016'
    assert columns[4] == 'pct_change_2015_to_2016'


def test_min_and_max_pct_change():
    data = crimeDataAnalyzer.import_data()

    assert crimeDataAnalyzer.max_pct_change(data) == 45.14
    assert crimeDataAnalyzer.max_pct_change(data) != 60
    assert crimeDataAnalyzer.min_pct_change(data) == 4.27
    assert crimeDataAnalyzer.min_pct_change(data) != 8

