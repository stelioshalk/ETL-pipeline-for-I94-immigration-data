# ETL-pipeline-for-I94-immigration-data

# Project description
This project is to design a data lake ETL solution that automates data cleansing and processing the I-94 Immigration Dataset. Additionaly, the the Global Temperature Dataset and U.S. City Demographic Dataset are used. 

# The Data sets
## I-94 Dataset
For decades, U.S. immigration officers issued the I-94 Form (Arrival/Departure Record) to foreign visitors (e.g., business visitors, tourists and foreign students) who lawfully entered the United States. The I-94 was a small white paper form that a foreign visitor received from cabin crews on arrival flights and from U.S. Customs and Border Protection at the time of entry into the United States. It listed the traveler's immigration category, port of entry, data of entry into the United States, status expiration date and had a unique 11-digit identifying number assigned to it. Its purpose was to record the traveler's lawful admission to the United States.

This data is stored as a set of SAS7BDAT files. SAS7BDAT is a database storage file created by Statistical Analysis System (SAS) software to store data. It contains binary encoded datasets used for advanced analytics, business intelligence, data management, predictive analytics, and more. The SAS7BDAT file format is the main format used to store SAS datasets.
The immigration data is partitioned into monthly SAS files. The data provided represents 12 months of data for the year 2016. This is the main dataset used in the project.

## Global Temperature Dataset
The Berkeley Earth Surface Temperature Study has created a preliminary merged data set by combining 1.6 billion temperature reports from 16 preexisting data archives.
The dataset is available here: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data

## The US Cities: Demographics Dataset
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
This data comes from the US Census Bureau's 2015 American Community Survey.
This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau.
The dataset is available here: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/

This data will be combined with port city data to provide ancillary demographic info for port cities.

 
## Airports Data
The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia).

Airport codes from around the world. Downloaded from public domain source http://ourairports.com/data/ who compiled this data from multiple different sources. This data is updated nightly.
The definition of the dataset is described here: https://ourairports.com/help/data-dictionary.html

## Additional data
A data dictionary file for immigration was provided which was splitted to the follwing files:
- i94cntyl.txt
- i94prtl.txt 
- i94addrl.txt
- i94model.txt
- I94VISA.txt
The above files were converted to csv files. The biggest ones were converted by calling the generate_csv.py script.

# Architecture of the solution
The solution initially processes the raw datasets,and stores them as parquet files in Amazon S3.Some basic filtering gets applied before loading the data as parquet files. Apache spark is used for filtering and loading the data to S3.
After storing the files in S3, the parquet files get linked as external tables in an Amazon redshift staging database. Finally, the external staging tables are used for populating the tables of the model.

The diagram below reflects the main data flow of the solution:
<img src="/images/model.jpg">

This is the structure of the external tables that are linked with the parquet files stored in an S3 bucket:
<img src="/images/staging.jpg">


<br><br>
Those are the fact and dimension tables of the model.
<img src="/images/model-tables.jpg">





# Data cleaning
the follwing data cleansing has been applied. Some of the cleansing was taking place during extraction and loading the data to the staging parquet files in S3, and some other filtering has been applied while loading the fact and dimension tables of the model.

1. Filter temperature data to only use data for United States.
2. Remove irregular ports from I94 data.
3. Drop rows with missing IATA codes from I94 data.


