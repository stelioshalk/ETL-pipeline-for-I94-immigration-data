from pyspark.sql import SparkSession
import configparser
from subprocess import call
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def store_parquet_files():
    """This method stores staging data as parquet files in S3"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
   
    #Filter GlobalLandTemperaturesByState temperature data to only use data for United States.
    temperature_fname = 'csvfiles/GlobalLandTemperaturesByState.csv'
    country_temp_df=spark.read.option("header", "true").csv(temperature_fname)
    filtered_df=country_temp_df.where("Country == 'United States'")
    filtered_df.write.parquet(path="s3a://shalbucket/staging_GlobalLandTemperaturesByState.parquet", mode = "overwrite")
    
    #create the staging_i94prtl
    staging_i94prtl=spark.read.option("header", "true").csv('csvfiles/i94prtl.csv')
    staging_i94prtl.write.parquet(path="s3a://shalbucket/staging_i94prtl.parquet", mode = "overwrite")
        
    #create the staging_i94cntyl
    staging_i94cntyl=spark.read.option("header", "true").csv('csvfiles/i94cntyl.csv')
    staging_i94cntyl.write.parquet(path="s3a://shalbucket/staging_i94cntyl.parquet", mode = "overwrite")

    #create the staging_i94addrl
    staging_i94addrl=spark.read.option("header", "true").csv('csvfiles/i94addrl.csv')
    staging_i94addrl.write.parquet(path="s3a://shalbucket/staging_i94addrl.parquet", mode = "overwrite")
    #create the staging_i94model
    staging_i94model=spark.read.option("header", "true").csv('csvfiles/i94model.csv')
    staging_i94model.write.parquet(path="s3a://shalbucket/staging_i94model.parquet", mode = "overwrite")

    #create the staging_airport_codes
    staging_airport_codes=spark.read.option("header", "true").csv('csvfiles/airport-codes_csv.csv')
    staging_airport_codes.write.parquet(path="s3a://shalbucket/staging_airport_codes.parquet", mode = "overwrite")

    #create the staging_i94visa
    staging_i94visa=spark.read.option("header", "true").csv('csvfiles/i94visa.csv')
    staging_i94visa.write.parquet(path="s3a://shalbucket/staging_i94visa.parquet", mode = "overwrite")
    #filter the i94 dataset
    
    #Remove irregular ports from I94 data.
    df_spark =spark.read.load('./sas_data')
    df_spark.createOrReplaceTempView('raw_immigrations')
    allowed_ports=spark.read.option("header", "true").csv('csvfiles/i94prtl.csv')
    allowed_ports.createOrReplaceTempView('staging_i94prtl')

    staging_immigrations_table=spark.sql("""
    SELECT * from raw_immigrations where i94port in (SELECT airport_code from staging_i94prtl)
    """)
    #create the staging_immigrations in s3 as parquet
    staging_immigrations_table.write.parquet("s3a://shalbucket/staging_immigrations.parquet", mode = "overwrite")

    #create the staging_demographics dataset by processing the us-cities-demographics.csv
    staging_demographics=spark.read.option("header", "true").option("delimiter", ";").csv('csvfiles/us-cities-demographics.csv')

    #When saving the file to Parquet format, you cannot use spaces and some specific characters.
    newColumns = []
    problematic_chars = ',;{}()='
    for c in staging_demographics.columns:
        c = c.lower()
        c = c.replace(' ', '_')
        for i in problematic_chars:
            c = c.replace(i, '')
        newColumns.append(c) 
    staging_demographics = staging_demographics.toDF(*newColumns)
    staging_demographics.write.parquet(path="s3a://shalbucket/staging_dmographics.parquet",mode = "overwrite")
    

def create_csv_files():
    """This method generates somw auxilary csv files by processing raw data in text files"""
    i94cntylFile = open('txtfiles/i94cntyl.txt', 'r')
    i94cntylFilecsv = open('i94cntyl.csv', 'w')
    try:
        while True:
            line = i94cntylFile.readline() 
            if not line:
                break
            list=line.split("=")
            newline=list[0].strip()+","+list[1].replace("'","").replace(","," ")
            i94cntylFilecsv.write(newline)
    except IndexError:
        print("empty line->"+line)
    i94cntylFilecsv.close()

    #process i94addrl.txt
    i94addrlFile = open('txtfiles/i94addrl.txt', 'r')
    i94addrlFilecsv = open('i94addrl.csv', 'w')
    try:
        while True:
            line = i94addrlFile.readline()
            if not line:
                break
            list=line.split("=")
            newline=list[0].strip().replace("'","")+","+list[1].replace("'","").replace(","," ")
            i94addrlFilecsv.write(newline)
    except IndexError:
        print("empty line->"+line)
    i94addrlFilecsv.close()

    #process i94prtl.txt
    i94prtlFile = open('txtfiles/i94prtl.txt', 'r')
    i94prtllFilecsv = open('i94prtl.csv', 'w')
    try:
        while True:
            line = i94prtlFile.readline()
            if not line:
                break
            list=line.split("=")       
            arport_code=list[0].strip().replace("'","")        
            secondpart=list[1].split(",")        
            city=secondpart[0].strip().replace("'","")
            if len(secondpart)==2:
                state_code=secondpart[1].replace("'","").replace(" ","")
            else:
                state_code=""
            newline=arport_code+","+city+","+state_code#+"\n"
            i94prtllFilecsv.write(newline)
    except IndexError:
        print("empty line->"+line)
        i94prtllFilecsv.close()

def main():
    """The main function of the etl script. """
    
    create_csv_files()
    store_parquet_files()
    
    #create the external staging tables and load the model with data from the staging tables 
    #using redshift glue.
    # You need to assign the AWSGlueConsoleFullAccess policy to the role
    call(["python", "create_tables.py"])
    
if __name__ == "__main__":
    main()
            