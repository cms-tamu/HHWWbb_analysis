# Import stuff
import os, sys, shutil, datetime, getpass
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utilities.Samples_and_Functions as sf
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression

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

if not os.path.exists(sf.RDDpath + "/DYtrain"):
    os.makedirs(sf.RDDpath + "/DYtrain")

# Parameters
selection = "(isMuMu==0) OR (isElEl==0)"
genbb_selection = "(genjet1_partonFlavour == 5 && genjet2_partonFlavour == 5)"
gencc_selection = "(genjet1_partonFlavour == 4 && genjet2_partonFlavour == 4)"
sigSelection = genbb_selection + " || " + gencc_selection
bkgSelection = "! " + sigSelection
now = datetime.datetime.now()
name_suffix = "miniAOD2RDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)
#Feature and samples
features = ["jet1_pt", "jet1_eta", "jet2_pt", "jet2_eta", "jj_pt", "ll_pt", "ll_eta", "llmetjj_DPhi_ll_met", "ht", "nJetsL"]
df_DYToLL_M10t50_db = spark.read.load(sf.pathDYdf + "df_DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_0J_db = spark.read.load(sf.pathDYdf + "df_DYToLL_0J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_1J_db = spark.read.load(sf.pathDYdf + "df_DYToLL_1J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_2J_db = spark.read.load(sf.pathDYdf + "df_DYToLL_2J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
# Merge in a single DF and perform the selection
frames = [df_DYToLL_M10t50_db, df_DYToLL_M50_0J_db, df_DYToLL_M50_1J_db, df_DYToLL_M50_2J_db]
df_DY  = df_DYToLL_M10t50_db.union(df_DYToLL_M50_0J_db).union(df_DYToLL_M50_1J_db).union(df_DYToLL_M50_2J_db)
df_DY  = df_DY.where(selection)

# Test And Training
#X_train, X_test, y_train, y_test = train_test_split( df_DY, df_??,
#                                                     train_size=0.7, test_size=0.3,
#                                                     random_state=4)
# Scaler for better training
#scaler = StandardScaler()
#X_train = scaler.fit_transform(X_train)
#X_test = scaler.transform(X_test)

model = LogisticRegression(penalty='l2', dual=False,  tol=0.0001, C=1.0, fit_intercept=True, intercept_scaling=1, class_weight=None,
                           random_state=None, solver='liblinear',max_iter=100, multi_class='ovr',verbose=0, warm_start=False, n_jobs=1)
#Train
#mod.fit(X_train, y_train)
#scores = mod.score(X_train, y_train)
#Predict
#y_rf = mod.predict(X_test)

