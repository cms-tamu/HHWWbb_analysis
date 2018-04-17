# Import stuff
import os, sys, shutil, datetime, getpass
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

# Personalize outputname
now = datetime.datetime.now()
name_suffix = "miniAOD2RDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)

# Read the ROOT file into a Spark DataFrame...
df_TT = spark.read.format("org.dianahep.sparkroot").load(sf.TT_file)
df_S_Grav500 = spark.read.format("org.dianahep.sparkroot").load(sf.S_Grav500_file)

#use the following to drop variables you will not use
var_todrop = [] #to be finalized
for iVar in var_todrop:
    df_TT = df_TT.drop(iVar)
    df_S_Grav500 = df_S_Grav500.drop(iVar)
print "The Variables you have are: "
df_TT.printSchema()
#from pyspark import SparkContext
#sc = SparkContext.getOrCreate()
#df_TT = sc.parallelize(df_TT.collect())
#df_S_Grav500 = sc.parallelize(df_S_Grav500.collect())

# Let's make a basic selection
print '------------------------SELECETION------------------------'
print "You have ", df_TT.count(), "events in TT"
print "You have ", df_S_Grav500.count(), "events in Grav_500"
Selection = 'lep1_pt>20 and lep2_pt>20 and ll_M>76 and ll_M<106 and HME>250'
df_TT        = df_TT.where(Selection)
df_S_Grav500 = df_S_Grav500.where(Selection)
print "After selection you have ", df_TT.count(), "events in TT"
print "After selection you have ", df_S_Grav500.count(), "events in Grav_500"

#Now Saving the dataframe locally
print '------------------------SAVING RDD------------------------'
df_S_Grav500.write.format("com.databricks.spark.csv").option("header", "true").save( sf.RDDpath + name_suffix + "df_Grav500.csv" )
df_TT.write.format("com.databricks.spark.csv").option("header", "true").save( sf.RDDpath + name_suffix + "df_TT.csv" )
