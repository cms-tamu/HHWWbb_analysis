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
import matplotlib.pyplot as plt
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gROOT.Reset()
# Start up spark and get our SparkSession... the lines below specify the dipendencies
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit, rand
spark = SparkSession.builder \
    .appName(# Name of your application in the dashboard/UI
             "spark-analyzeRDD"
            ) \
    .config(# Tell Spark to load some extra libraries from Maven (the Java repository)
            'spark.jars.packages',
            'org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4,org.diana-hep:histogrammar_2.11:1.0.4'
            ) \
    .getOrCreate()
# Creating needed folders
if not os.path.exists(sf.RDDpath + '/DYfigures'):
    os.makedirs(sf.RDDpath + '/DYfigures')

# Parton information
flavors = ["b", "c", "l"]
partonidmap  = {"b":5, "c":4, "l":0}
# Hisotgrams
binning = np.asarray([-0.4325139551124535, -0.2146539640268055, -0.17684879232551598, -0.1522156780133781, -0.13344360493544538, -0.1177783085968212, -0.10431773748076387, -0.09240803627202236, -0.08144732988778663, -0.07139562851774808, -0.06195872754019471, -0.053149265226606804, -0.044689436819594426, -0.036486494035769285, -0.028370020384749492, -0.02052289170780913, -0.01265119174726717, -0.004810595256756055, 0.003258152851774066, 0.01125285685430063, 0.019322492143167114, 0.02785483333896287, 0.03659553016370119, 0.04591206104108278, 0.05601279709011762, 0.06690819726322504, 0.07861467402378061, 0.09302953795299788, 0.11151410228370977, 0.13829367256021688, 0.333748766143408],
        dtype='float')
n_bins = len(binning) - 1
# Load what you have trained
X_train = pd.read_csv('models/X_train.csv')
X_train.columns = sf.DYfeatures + sf.DYneeded_vars
X_test = pd.read_csv('models/X_test.csv')
X_test.columns = sf.DYfeatures + sf.DYneeded_vars
y_train = pd.read_csv('models/y_train.csv')
y_test = pd.read_csv('models/y_test.csv')
model = joblib.load('models/DYmodel.pkl') 

# Predict
y_rf = model.predict_proba(X_test[sf.DYfeatures]) #Return [prob_back, prob_sig]
ProbS = y_rf[:,1] # Transform the probability between -0.5 and 0.5
ProbS = ProbS - 0.5

# Plots
efficiencies = {}
for flav1 in flavors:
    for flav2 in flavors:
        name = "%s%s_frac" % (flav1, flav2)
        frac = ROOT.TEfficiency(name, name, n_bins, binning)
        frac.SetStatisticOption(ROOT.TEfficiency.kBUniform)
        frac.SetUseWeightedEvents()
        # Set global weight for sample
        #frac.SetWeight(cross_section / event_wgt_sum)
        key = (flav1, flav2)
        efficiencies[key] = frac

print y_rf.shape
for i in range(0, y_rf.shape[0]):
    if (i % 100 == 0):
        print("Event %d over %d" % (i + 1, y_rf.shape[0]))
    # Weight: take into account? Also lepton ID SF?
    weight = float(X_test["sample_weight"].iloc[i]) * float(X_test["event_reco_weight"].iloc[i])
    def pass_flavor_cut(flav1, flav2):
        return int(X_test['genjet1_partonFlavour'].iloc[i]) == int(partonidmap[flav1]) and int(X_test['genjet2_partonFlavour'].iloc[i]) == int(partonidmap[flav2])
    for flav1 in flavors:
        for flav2 in flavors:
            key = (flav1, flav2)
            efficiencies[key].FillWeighted(pass_flavor_cut(flav1, flav2), weight, ProbS[i])

print("Done")
output = ROOT.TFile.Open("outDYtesting.root", "recreate")
for key, value in efficiencies.items():
    value.Write()
output.Close()

#Plots
#ProbS.loc[y_test['0'] == 1]

#plt.hist(ProbS)
#plt.ylabel('Probability')
#plt.show()
#plt.scatter(ProbS, X_train2[:, 1])

