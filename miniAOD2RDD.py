# Import stuff
import os, sys
import pandas
import utilities.Samples_and_Functions as sf
#execfile("utilities/Samples_and_Functions.py")

# Start up spark and get our SparkSession... the lines below specify the dipendencies
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder \
    .appName(# Name of your application in the dashboard/UI
             "00-test-spark"
            ) \
    .config(# Tell Spark to load some extra libraries from Maven (the Java repository)
            'spark.jars.packages',
            'org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4'
            ) \
    .getOrCreate()

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
df_S_Grav500.toPandas().to_csv('/data/MDD/df_S_Grav500.csv')
df_TT.toPandas().to_csv('/data/MDD/df_TT.csv')
