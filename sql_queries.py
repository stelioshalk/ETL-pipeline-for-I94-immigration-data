import configparser


# CONFIG
#config = configparser.ConfigParser()
#config.read('dl.cfg')

immigrations_table_drop = "DROP TABLE IF EXISTS immigrations"
airport_table_drop = "DROP TABLE IF EXISTS airport"
transportation_mode_table_drop = "DROP TABLE IF EXISTS transportation_mode"
country_drop = "DROP TABLE IF EXISTS country"
state_drop = "DROP TABLE IF EXISTS state"
visa_status_drop="DROP TABLE IF EXISTS visa_status"

immigrations_table_create= ("""CREATE TABLE IF NOT EXISTS immigrations(
    cicid integer NOT NULL,
    i94yr integer,
    i94mon integer,
    i94cit integer,
    i94res integer,
    i94port character varying(5) ,
    arrdate character varying,
    i94mode integer,
    i94addr character varying(5),
    depdate character varying(10),
    i94bir integer,
    i94visa integer,
    visapost character varying(5),
    occup character varying(10),
    entdepa character varying(5),
    entdepd character varying(5),
    entdepu character varying(5),
    biryear integer,
    gender character varying(2),
    airline character varying(5),
    admnum integer,
    fltno character varying(10),
    visatype character varying(5),
    CONSTRAINT immigrations_pkey PRIMARY KEY (cicid)
    ); 
""")
                            
airport_table_create=("""CREATE TABLE IF NOT EXISTS airport(
    airport_code character varying(5)  NOT NULL,
    city character varying(40) ,
    state_code character varying(10) ,
    airport_name character varying(40) ,
    airport_type character varying(40),
    CONSTRAINT airport_pkey PRIMARY KEY (airport_code)
    ); 
""") 

transportation_mode_table_create=("""CREATE TABLE IF NOT EXISTS transportation_mode(
    code integer NOT NULL,
    transportation_mode character varying(10),
	CONSTRAINT transportation_mode_pkey PRIMARY KEY (transportation_mode)
    ); 
""")                            

country_table_create=("""CREATE TABLE IF NOT EXISTS country(
	country_code character varying(5),
	country_name character varying(25),
	CONSTRAINT country_pkey PRIMARY KEY(country_code)
    ); 
""")   

state_table_create=("""CREATE TABLE IF NOT EXISTS state(
    state_id character varying(5),
    state_name character varying(20),
    male_population integer,
    foreign_born integer,
    median_age float,
    avg_household_size float,
    average_temperature float,
    CONSTRAINT state_pkey PRIMARY KEY(state_id)
    ); 
""") 

visa_status_table_create=("""create table visa_status(
code varchar(10), visa_type varchar(20))""")

Create_externalSchemaSQL="""CREATE EXTERNAL SCHEMA IF NOT EXISTS staging
FROM data catalog DATABASE 'dev'
IAM_ROLE 'arn:aws:iam::579194515839:role/myRedshiftRole' 
create external database if not exists;"""

#using redshift glue
# need to assign the AWSGlueConsoleFullAccess policy to the reole
stagingDemographicsSQL="""create external table staging.staging_demographics(
city varchar(100),
state varchar(100),
median_age varchar(100),
male_population varchar(100),
total_population varchar(100),
number_of_veterans varchar(100),
"foreign-born" varchar(100),
average_household_size varchar(100),
state_code varchar(100),
race varchar(100),
count varchar(100)
) 
stored as parquet location 's3://shalbucket/staging_dmographics.parquet';"""

staging_i94prtlSQL="""create external table staging.staging_i94prtl(
airport_code varchar(10),
city varchar(100),
state_code varchar(10)
)
stored as parquet location 's3://shalbucket/staging_i94prtl.parquet';""" 

staging_i94modelSQL="""create external table staging.staging_i94model(
code varchar(10),
transportation_mode varchar(10)
) 
stored as parquet location 's3://shalbucket/staging_i94model.parquet';"""                    

staging_airport_codesSQL="""create external table staging.staging_airport_codes(
id varchar(10),
type varchar(20),
name varchar(20),
elevation varchar(20),
continent varchar(20),
iso_country varchar(20),
iso_region varchar(20),
municipality varchar(20),
gps_code varchar(100),
iata_code varchar(10),
local_code varchar(10),
coordinates varchar(100)
)
stored as parquet location 's3://shalbucket/staging_airport_codes.parquet';"""
                  
staging_i94visaSQL="""create external table staging.staging_i94visa(
code varchar(10),
visa_type varchar(10)
)
stored as parquet location 's3://shalbucket/staging_i94visa.parquet';"""

staging_i94cntylSQL="""create external table staging.staging_i94cntyl(
country_code varchar(10),
country_name varchar(20)
)
stored as parquet location 's3://shalbucket/staging_i94cntyl.parquet';"""

staging_GlobalLandTemperaturesByStatelSQL="""create external table staging.staging_GlobalLandTemperaturesByState(
dt varchar(10),
AverageTemperature varchar(20),
AverageTemperatureUncertainty varchar(20),
State varchar(20),
Country varchar(20)
)
stored as parquet location 's3://shalbucket/staging_GlobalLandTemperaturesByState.parquet';"""


staging_i94addrlSQL="""create external table staging.staging_i94addrl(
state_id varchar(10),
state_name varchar(20)
)
stored as parquet location 's3://shalbucket/staging_i94addrl.parquet';"""


staging_immigrationSQL="""create external table staging.staging_immigration(
CICID DOUBLE PRECISION,
I94YR DOUBLE PRECISION,
I94MON  DOUBLE PRECISION,
I94CIT  DOUBLE PRECISION,
I94RES  DOUBLE PRECISION,
I94PORT varchar(40),
ARRDATE DOUBLE PRECISION,
I94MODE DOUBLE PRECISION,
I94ADDR varchar(40),
DEPDATE DOUBLE PRECISION,
I94BIR DOUBLE PRECISION,
I94VISA  DOUBLE PRECISION,
COUNT  DOUBLE PRECISION,
DTADFILE varchar(100),
VISAPOST varchar(40),
OCCUP varchar(20),
ENTDEPA varchar(20),
ENTDEPD varchar(20),
ENTDEPU varchar(20),
MATFLAG varchar(20),
BIRYEAR DOUBLE PRECISION,
DTADDTO varchar(20),
GENDER varchar(20),
ISNUM varchar(20),
AIRLINE varchar(40),
ADNUM DOUBLE PRECISION,
FLTNO varchar(20),
VISATYPE varchar(20)
)
stored as parquet location 's3://shalbucket/staging_immigrations.parquet';"""


drop_staging_demographics="""drop table if exists staging.staging_demographics;"""
drop_staging_i94model="""drop table if exists staging.staging_i94model;"""
drop_staging_i94prtl="""drop table if exists staging.staging_i94prtl;"""
drop_staging_airport_codes="""drop table if exists staging.staging_airport_codes;"""
drop_staging_i94visa="""drop table if exists staging.staging_i94visa;"""
drop_staging_i94cntyl="""drop table if exists staging.staging_i94cntyl;"""
drop_staging_GlobalLandTemperaturesByState="""drop table if exists staging.staging_GlobalLandTemperaturesByState;"""
drop_staging_i94addrl="""drop table if exists staging.staging_i94addrl;"""
drop_staging_immigration="""drop table if exists staging.staging_immigration;"""

load_country_sql="""insert into country (country_code,country_name)
(select country_code,country_name from staging.staging_i94cntyl); """ 

load_airport_sql="""insert into airport (airport_code,city,state_code,airport_name,airport_type)
select a.iata_code,p.city,p.state_code,a.name,a.type from staging.staging_i94prtl p
left join staging.staging_airport_codes a on p.airport_code=a.iata_code where a.iso_country='US'; """

load_transp_mode_sql="""insert into transportation_mode (code, transportation_mode)
(Select cast(code as integer),transportation_mode from staging.staging_i94model); """

load_visa_status="""insert into visa_status (code, visa_type)
(Select code, visa_type from staging.staging_i94visa); """

load_state_sql="""insert into state (state_id,state_name,male_population,foreign_born,median_age,avg_household_size,average_temperature)
select d.state_code,d.state,cast(d.male_population as integer),cast(d."foreign-born" as integer),cast (d.median_age as double precision),cast(d.average_household_size as double precision),  t.average_temprature
from staging.staging_demographics d
left join (SELECT avg(averagetemperature) as average_temprature ,country, state 
from staging.staging_globallandtemperaturesbystate
group by state,country) t
on d.state=t.state;"""

load_immigrations_sql="""
insert into immigrations(cicid,i94yr,i94mon,i94cit,i94res,i94port,arrdate,i94mode,i94addr,depdate,i94bir,i94visa,visapost)
(Select cicid,i94yr,i94mon,i94cit,i94res,i94port,arrdate,i94mode,i94addr,depdate,i94bir,i94visa,visapost
from staging.staging_immigration);"""

data_quality_check1_sql="""select count(*) from airport;"""
data_quality_check2_sql="""select count(*) from immigrations;"""

drop_table_queries =[immigrations_table_drop,airport_table_drop,transportation_mode_table_drop,country_drop,state_drop,visa_status_drop]
create_table_queries=[immigrations_table_create,airport_table_create,transportation_mode_table_create,country_table_create,state_table_create,visa_status_table_create]
create_staging_tables_queries=[Create_externalSchemaSQL,stagingDemographicsSQL,staging_i94prtlSQL,staging_i94modelSQL,staging_airport_codesSQL,staging_i94visaSQL,staging_i94cntylSQL,staging_GlobalLandTemperaturesByStatelSQL,staging_i94addrlSQL,staging_immigrationSQL]
drop_staging_tables_queries=[Create_externalSchemaSQL,drop_staging_demographics,drop_staging_i94model,drop_staging_i94prtl,drop_staging_airport_codes,drop_staging_i94visa,drop_staging_i94cntyl,drop_staging_GlobalLandTemperaturesByState,drop_staging_i94addrl,drop_staging_immigration]

load_model_data_queries=[load_country_sql,load_transp_mode_sql,load_immigrations_sql,load_state_sql,load_airport_sql,load_visa_status]
data_quality_checks_queries=[data_quality_check1_sql,data_quality_check2_sql]

