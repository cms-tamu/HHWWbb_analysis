# Import stuff
import os, sys, shutil
import pandas
import utilities.Samples_and_Functions as sf

# Start up spark and get our SparkSession... the lines below specify the dipendencies
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder \
    .appName(# Name of your application in the dashboard/UI
             "spark-miniAOD2RDD"
            ) \
    .config(# Tell Spark to load some extra libraries from Maven (the Java repository)
            'spark.jars.packages',
            'org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4'
            ) \
    .getOrCreate()
# 
#print str(spark.debug.maxToStringFields)
#spark.debug.maxToStringFields=50

# Read the ROOT file into a Spark DataFrame...
df_TT = spark.read.format("org.dianahep.sparkroot").load(sf.TT_file)
df_S_Grav500 = spark.read.format("org.dianahep.sparkroot").load(sf.S_Grav500_file)
print "The Variables you have are: "
df_TT.printSchema()
#use the following to drop variables you will not use
#df_TT = df_TT.drop()
#df_S_Grav500 = df_S_Grav500.drop()

# Let's make a basic selection
print "You have ", df_TT.count(), "events in TT"
print "You have ", df_S_Grav500.count(), "events in Grav_500"
Selection = 'lep1_pt>20 and lep2_pt>20 and ll_M>76 and ll_M<106 and HME>250'
df_TT        = df_TT.where(Selection)
df_S_Grav500 = df_S_Grav500.where(Selection)
print "After selection you have ", df_TT.count(), "events in TT"
print "After selection you have ", df_S_Grav500.count(), "events in Grav_500"

#Now Saving the dataframe locally
shutil.rmtree( sf.MDDpath + "df_Grav500.csv" )
df_S_Grav500.write.format("com.databricks.spark.csv").option("header", "true").save( sf.MDDpath + "df_Grav500.csv" )
shutil.rmtree( sf.MDDpath + "df_TT.csv" )
df_TT.write.format("com.databricks.spark.csv").option("header", "true").save( sf.MDDpath + "df_TT.csv" )
