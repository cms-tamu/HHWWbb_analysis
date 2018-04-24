# Import stuff
import os, sys, shutil, datetime, getpass
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utilities.Samples_and_Functions as sf
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.externals import joblib
# Start up spark and get our SparkSession... the lines below specify the dipendencies
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit, rand, sum
spark = SparkSession.builder \
    .appName(# Name of your application in the dashboard/UI
             "spark-analyzeRDD"
            ) \
    .config(# Tell Spark to load some extra libraries from Maven (the Java repository)
            'spark.jars.packages',
            'org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4,org.diana-hep:histogrammar_2.11:1.0.4'
            ) \
    .getOrCreate()

if not os.path.exists(sf.RDDpath + '/DYtrain'):
    os.makedirs(sf.RDDpath + '/DYtrain')
if not os.path.exists('models/'):
    os.makedirs('models/')
# Parameters and Selections
selection = "(isMuMu>0) OR (isElEl>0)"
genbb_selection = "((genjet1_partonFlavour == 5) AND (genjet2_partonFlavour == 5))" #DY + bb
gencc_selection = "((genjet1_partonFlavour == 4) AND (genjet2_partonFlavour == 4))" #DY + cc
sigSelection = "(" + genbb_selection + " OR " + gencc_selection + ")" # DY + bb or cc
bkgSelection = "NOT" + sigSelection
now = datetime.datetime.now()
name_suffix = "miniAOD2RDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)
#Feature and samples ans weights
features = ["jet1_pt", "jet1_eta", "jet2_pt", "jet2_eta", "jj_pt", "ll_pt", "ll_eta", "llmetjj_DPhi_ll_met", "ht", "nJetsL"]
df_DYToLL_M10t50  = spark.read.load(sf.pathDYdf + "df_DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_0J  = spark.read.load(sf.pathDYdf + "df_DYToLL_0J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_1J  = spark.read.load(sf.pathDYdf + "df_DYToLL_1J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
df_DYToLL_M50_2J  = spark.read.load(sf.pathDYdf + "df_DYToLL_2J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")

# Compute the weights
DYToLL_M10t50 = sf.get_DYweights("DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8")
DYToLL_M50_0J = sf.get_DYweights("DYToLL_0J_13TeV-amcatnloFXFX-pythia8")
DYToLL_M50_1J = sf.get_DYweights("DYToLL_1J_13TeV-amcatnloFXFX-pythia8")
DYToLL_M50_2J = sf.get_DYweights("DYToLL_2J_13TeV-amcatnloFXFX-pythia8")
df_DYToLL_M10t50 = df_DYToLL_M10t50.withColumn('cross_section', lit(DYToLL_M10t50["cross_section"]))
df_DYToLL_M10t50 = df_DYToLL_M10t50.withColumn('relativeWeight', lit(DYToLL_M10t50["relativeWeight"]))
df_DYToLL_M50_0J = df_DYToLL_M50_0J.withColumn('cross_section', lit(DYToLL_M50_0J["cross_section"]))
df_DYToLL_M50_0J = df_DYToLL_M50_0J.withColumn('relativeWeight', lit(DYToLL_M50_0J["relativeWeight"]))
df_DYToLL_M50_1J = df_DYToLL_M50_1J.withColumn('cross_section', lit(DYToLL_M50_1J["cross_section"]))
df_DYToLL_M50_1J = df_DYToLL_M50_1J.withColumn('relativeWeight', lit(DYToLL_M50_1J["relativeWeight"]))
df_DYToLL_M50_2J = df_DYToLL_M50_2J.withColumn('cross_section', lit(DYToLL_M50_2J["cross_section"]))
df_DYToLL_M50_2J = df_DYToLL_M50_2J.withColumn('relativeWeight', lit(DYToLL_M50_2J["relativeWeight"]))
# Merge in a single DF and perform the selection
df_DY  = df_DYToLL_M10t50.union(df_DYToLL_M50_0J).union(df_DYToLL_M50_1J).union(df_DYToLL_M50_2J)
df_DY  = df_DY.where(selection)

def computeWeight(event_reco_weight, relativeWeight):
    return event_reco_weight * relativeWeight
weightUDF = udf(computeWeight, FloatType())
df_DY = df_DY.withColumn("weightExpr", weightUDF("event_reco_weight","relativeWeight"))
#Now define Signal and background 
df_DY_sig = df_DY.where(sigSelection)
df_DY_bac = df_DY.where(bkgSelection)
#Sig and Bacl should have same total weight
TotSweight = float(str(df_DY_sig.groupBy().agg(sum("weightExpr")).collect())[21:-3])
TotBweight = float(str(df_DY_bac.groupBy().agg(sum("weightExpr")).collect())[21:-3])
df_DY_sig = df_DY_sig.withColumn("weightExpr_norm", df_DY_sig.weightExpr/TotSweight).drop("weightExpr")
df_DY_bac = df_DY_bac.withColumn("weightExpr_norm", df_DY_bac.weightExpr/TotBweight).drop("weightExpr")
print df_DY_sig.groupBy().agg(sum("weightExpr_norm")).collect()
print df_DY_bac.groupBy().agg(sum("weightExpr_norm")).collect()

# Add the Classification depending on the category (-0.5 / 0.5)
df_DY_sig = df_DY_sig.withColumn("Y", lit(1))
df_DY_bac = df_DY_bac.withColumn("Y", lit(-1))
# Merge in a single DF and shuffle
df_DY = df_DY_sig.union(df_DY_bac)
df_DY = df_DY.orderBy(rand())
# Now divide Features, Classes, and weights
df_DY_X = df_DY.select(features)
df_DY_Y = df_DY.select("Y")
df_DY_w = df_DY.select("weightExpr_norm")
# Transform pySpark datafram to pandas df, so that you can use sklearn
df_DY_X = df_DY_X.toPandas()
df_DY_Y = df_DY_Y.toPandas()
df_DY_w = df_DY_w.toPandas()

# Test And Training
X_train, X_test, y_train, y_test, w_train, w_test = train_test_split( df_DY_X, df_DY_Y, df_DY_w,
                                                                      train_size=0.7, test_size=0.3,
                                                                      random_state=4)
# Scaler for better training
scaler  = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test  = scaler.transform(X_test)
y_train = np.ravel(y_train) # Return a contiguous flattened array
y_test  = np.ravel(y_test)

# Note that the weight can only be given per class, not per event.
model = LogisticRegression(penalty='l2', dual=False,  tol=0.0001, C=1.0, fit_intercept=True, intercept_scaling=1, class_weight=None,
                           random_state=None, solver='liblinear',max_iter=100, multi_class='ovr',verbose=0, warm_start=False, n_jobs=1)
#model = SVC(C=1.0, kernel='rbf', degree=3, gamma='auto', coef0=0.0, shrinking=True, probability=False, tol=0.001,
#            cache_size=200, class_weight=None, verbose=False, max_iter=-1, decision_function_shape='ovr', random_state=None)
# Train
print "Fitting"
model.fit(X_train, y_train)
print "Score is:", model.score(X_train, y_train)
# Save your model
print "Saving the model and the dataframe..."
pd.DataFrame(X_train).to_csv("models/X_train.csv",index = False,header=True)
pd.DataFrame(X_test).to_csv("models/X_test.csv",index = False,header=True)
pd.DataFrame(y_train).to_csv("models/y_train.csv",index = False,header=True)
pd.DataFrame(y_test).to_csv("models/y_test.csv",index = False,header=True)
pd.DataFrame(w_train).to_csv("models/w_train.csv",index = False,header=True)
pd.DataFrame(w_test).to_csv("models/w_test.csv",index = False,header=True)
joblib.dump(model, 'models/DYmodel.pkl')
print "THE END."
