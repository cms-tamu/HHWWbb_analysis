# Import stuff
import os, sys, math, shutil, datetime, getpass
import pandas
import utilities.Samples_and_Functions as sf
import matplotlib.pyplot as plt

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
#print str(spark.debug.maxToStringFields)
#spark.debug.maxToStringFields=50

# Personalize outputname
now = datetime.datetime.now()
name_suffix = str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)

# Read the ROOT file into a Spark DataFrame...
df_TT = spark.read.load(sf.TT_df, format="csv", sep=",", inferSchema="true", header="true")
df_S_Grav500 = spark.read.load(sf.S_Grav500_df, format="csv", sep=",", inferSchema="true", header="true")
print "The Variables you have are: "
df_TT.printSchema()
#drop variables you will not use
var_todrop = ["nu_top_pt"]
for iVar in var_todrop:
    df_TT = df_TT.drop(iVar)
    df_S_Grav500 = df_S_Grav500.drop(iVar)

# Let's make a basic selection
Selection = 'lep1_pt>20 and lep2_pt>20 and ll_M>76 and ll_M<106 and HME>250'
df_TT        = df_TT.where(Selection)
df_S_Grav500 = df_S_Grav500.where(Selection)
print "After selection you have ", df_TT.count(), "events in TT"
print "After selection you have ", df_S_Grav500.count(), "events in Grav_500"

# Let's add a new variable to the RDD. This can be done usoing a User Define Function
# Spark will execute the following function for each row. You can put arbitrary python
def DphiJet( jet1_phi, jet2_phi ):
    return math.fabs( jet1_phi - jet2_phi )
DphiJet_UDF = udf(DphiJet, DoubleType())
df_TT        = df_TT.withColumn("jet12_Dphi", DphiJet_UDF("jet1_phi", "jet2_phi"))
df_S_Grav500 = df_S_Grav500.withColumn("jet12_Dphi", DphiJet_UDF("jet1_phi", "jet2_phi"))

# Let's make a basic selection, and add a column of bool to see if the raw pass the selection
def sele_forDNN(ll_pt, met_pt, jet12_Dphi ):
    return (ll_pt > 30. and met_pt>30 and jet12_Dphi<300)
sele_forDNN_UDF = udf(sele_forDNN, BooleanType())
df_TT        = df_TT.withColumn("sele_DNN", sele_forDNN_UDF("ll_pt", "met_pt","jet12_Dphi"))
df_S_Grav500 = df_S_Grav500.withColumn("sele_DNN", sele_forDNN_UDF("ll_pt", "met_pt","jet12_Dphi"))
df_TT.printSchema()

#Data/MC plots
print '------------------------PLOTTING------------------------'
import matplotlib.pyplot as plt
import histogrammar as hg
import histogrammar.sparksql
hg.sparksql.addMethods(df_S_Grav500)
hg.sparksql.addMethods(df_TT)
#
h_ll_pt = df_S_Grav500.Bin(50, 50, 350, df_S_Grav500['ll_pt'])
ax = h_ll_pt.plot.matplotlib(name="Pt(l1+l2) [GeV]")
#plt.show()
plt.savefig('figures/h_ll_pt.png')

#Now Saving the dataframe locally
print '------------------------SAVING RDD------------------------'
df_S_Grav500.write.format("com.databricks.spark.csv").option("header", "true").save( sf.RDDpath + name_suffix + "Analysis_df_Grav500.csv" )
df_TT.write.format("com.databricks.spark.csv").option("header", "true").save( sf.RDDpath + name_suffix + "Analysis_df_TT.csv" )
