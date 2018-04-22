# Import stuff
import os, sys, shutil, datetime, getpass
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utilities.Samples_and_Functions as sf
import pandas as pd
import numpy as np

# Start up spark and get our SparkSession... the lines below specify the dipendencies
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
spark = SparkSession.builder \
    .appName(# Name of your application in the dashboard/UI
             "spark-analyzeRDD"
            ) \
    .config(# Tell Spark to load some extra libraries from Maven (the Java repository)
            'spark.jars.packages',
            'org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4,org.diana-hep:histogrammar_2.11:1.0.4'
            ) \
    .getOrCreate()

if not os.path.exists(sf.RDDpath + "/DYminiAOD2RDD"):
    os.makedirs(sf.RDDpath + "/DYminiAOD2RDD")

# Personalize outputname
now = datetime.datetime.now()
name_suffix = "miniAOD2RDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)

for iFile in os.listdir(sf.pathDYroot):
    if "_Friend.root" in iFile and os.path.isfile(sf.pathDYroot + "/" + iFile):
        print iFile
        df = spark.read.format("org.dianahep.sparkroot").load(sf.pathDYroot + "/" + iFile)
        var_todrop = [k for k in df.columns if ('alljets_' in k or 'Jet_mhtCleaning' in k )]
        for iVar in var_todrop:
            df = df.drop(iVar)
        #Now Saving the dataframe locally
        print '------------------------SAVING RDD------------------------'
        df.write.format("parquet").save( sf.RDDpath + "/DYminiAOD2RDD/" + name_suffix + "/df_" + iFile[:-5] + ".parquet" )

