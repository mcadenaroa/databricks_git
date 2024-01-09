# View the latest COVID-19 hospitalization data

# install requirements
pip install -r ./requirements.txt

dbutils.widgets.dropdown(name="run as", choices=["testing", "production"], defaultValue="testing")


run_as = dbutils.widgets.get("run as")
print(run_as)


from covid_analysis.transforms import *

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv"
path = "/tmp/covid-hospitalizations.csv"

get_data(url, path)


import pandas as pd

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv("/tmp/covid-hospitalizations.csv")
df = filter_country(df, country='USA')
df = pivot_and_clean(df, fillna=0)  
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

display(df)

# Write to Delta Lake
df.to_table(name=run_as+"_covid_analysis", mode='overwrite')

# Using Databricks visualizations and data profiling
display(spark.table(run_as+"_covid_analysis"))

# Using python
df.to_pandas().plot(figsize=(13,6), grid=True).legend(loc='upper left')


if run_as == "testing":
  spark.sql("DROP TABLE testing_covid_analysis")
