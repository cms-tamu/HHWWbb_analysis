# Import stuff
import os, sys, math, shutil, datetime, getpass
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utilities.Samples_and_Functions as sf
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn import tree
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, RobustScaler
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score
import numpy as np
import pandas as pd

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

#Parameters
Selection = 'll_M > 76.'
features  = ["hme_mean_reco","hme_entries_reco","jj_pt", "ll_pt", "ll_M", "ll_DR_l_l", "jj_DR_j_j", "llmetjj_DPhi_ll_jj", "llmetjj_minDR_l_j", "llmetjj_MTformula", "isSF"]
FilesToConsider=["df_TTTo2L2Nu_TuneCUETP8M2_ttHtranche3_13TeV-powheg-pythia8_final.root.csv",
                 "df_GluGluToRadionToHHTo2B2VTo2L2Nu_M-500_narrow_13TeV-madgraph-v2_final.root.csv"]

# Personalize outputname
now = datetime.datetime.now()
name_suffix = "analyzeRDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)

# Read the CSV file into a pandas datafame
df_TT      = spark.read.load(sf.pathCSV1 + FilesToConsider[0], format="csv", sep=",", inferSchema="true", header="true")
df_Grav500 = spark.read.load(sf.pathCSV1 + FilesToConsider[1], format="csv", sep=",", inferSchema="true", header="true")
#Keep only features
df_Grav500.printSchema()
df_TT      = df_TT.select(features)
df_Grav500 = df_Grav500.select(features)
# Let's make a basic selection
#df_TT      = df_TT.where(Selection)
#df_Grav500 = df_Grav500.where(Selection)
# Now Convert to pandas
df_TT      = df_TT.toPandas()
df_Grav500 = df_Grav500.toPandas()
df_TT = df_TT[features]
df_Grav500 = df_Grav500[features]

#Add if they are Signal or not
df_TT['sample'] = 1
df_Grav500['sample'] = 0
#Merge them into a single dataframe
frames = [df_TT, df_Grav500]
df_result = pd.concat(frames)
# Shuffle them (frac=1 means it shuffle each single line). Also you want to reset the index.
df_result = df_result.sample(frac=1).reset_index(drop=True)
print df_result.iloc[0:10]
print df_result.iloc[-10:]
df_result_X = df_result[features].as_matrix()
df_result_Y = df_result['sample']

# Test And Training
X_train, X_test, y_train, y_test = train_test_split( df_result_X, df_result_Y,
                                                     train_size=0.7, test_size=0.3,
                                                     random_state=4)
# Scaler for better training
scaler = StandardScaler() 
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

TrainModel=[True,True,True,True,True]
mod_name = ['DecisionTreeClassifier','RandomForestClassifier','AdaBoostClassifier','GradientBoostingClassifier','XGBClassifier']
models = [DecisionTreeClassifier(max_depth=None, criterion="entropy",splitter='best',class_weight={0: 0.7, 1: 0.3}),
          RandomForestClassifier(n_estimators=25, max_features="auto", max_depth=None, random_state=2, bootstrap=True, n_jobs=-1),
          AdaBoostClassifier(DecisionTreeClassifier(max_depth=None, criterion="entropy"), n_estimators=50),
          # Tune max_depth for best performance; n_estimators > 100 since small risk of overtraining
          GradientBoostingClassifier(loss='deviance', learning_rate=0.1, n_estimators=150, max_depth=10, criterion="friedman_mse"),
          XGBClassifier()]
if len(mod_name) != len(models) or len(mod_name) != len(TrainModel):
    print "Exiting... mod_name and models have a different size!" 
    sys.quit()

iT = 0 
for mod in models:
    if TrainModel[iT]:
        #Train
        mod.fit(X_train, y_train)   
        scores = mod.score(X_train, y_train)
        #Predict
        y_rf = mod.predict(X_test)
        w1 = np.ones(len(y_test))
        scores = mod.score(X_test, y_test)
        print "--------------------------",mod_name[iT],"--------------------------"
        print 'Signal efficiency:',        w1[(y_test == 0) & (y_rf == 0)].sum() / w1[y_test == 0].sum(),"(",w1[(y_test == 0) & (y_rf == 0)].sum(),"/",w1[y_test == 0].sum(),")"
        print 'Background efficiency:',    w1[(y_test == 1) & (y_rf == 1)].sum() / w1[y_test == 1].sum(),"(",w1[(y_test == 1) & (y_rf == 1)].sum(),"/",w1[y_test == 1].sum(),")"
        print 'Score:', scores
        if mod_name[iT]=="DecisionTreeClassifier":
            print "Sorting by importance: \n", sorted(zip(mod.feature_importances_,features), reverse=True)
            tree.export_graphviz(mod, out_file = mod_name[iT] + ".dot", feature_names = features)
            os.system("dot -Tsvg " + mod_name[iT] + ".dot -o " + mod_name[iT] + ".svg")
            os.system("rm -rf " + mod_name[iT] + ".dot")
        if mod_name[iT]=="AdaBoostClassifier":
            print "Sorting by importance: \n", sorted(zip(mod.feature_importances_,features), reverse=True)
        if mod_name[iT]=="XGBClassifier":
            print(mod)
        print "----------------------------------------------------"
    iT+=1

#plt.figure()
#binning, myrange, colors = 2, (0,1), ['red','blue']
#plt.hist( np.asarray(y_test), bins=binning, range=myrange, color=colors[0], alpha=0.3, density=True )
#plt.hist( np.asarray(y_rf), bins=binning, range=myrange, color=colors[1], alpha=0.3, density=True )
#plt.xlabel("Samples")
#plt.legend()
#plt.show()

###print '------------------------PLOTTING------------------------'
###import matplotlib.pyplot as plt
###import numpy as np
###import histogrammar as hg
###import histogrammar.sparksql
###hg.sparksql.addMethods(df_Grav500)
###hg.sparksql.addMethods(df_TT)
#### h_ll_pt
####h_ll_pt_Grav500 = df_Grav500.Bin(50, 50, 350, df_Grav500['ll_pt'])
####h_ll_pt_TT = df_TT.Bin(50, 50, 350, df_TT['ll_pt'])
####plt.hist( df_Grav500.select("ll_pt").collect(), bins=np.histogram(np.arange(50, 350, 50)) )
####ax = h_ll_pt_Grav500.plot.matplotlib(name="Pt(l1+l2) [GeV]")
###binning, myrange, colors = 20, (50.,350), ['red','blue']
###plt.hist( df_Grav500.select("ll_pt").rdd.flatMap(lambda x: x).collect(), bins=binning, range=myrange, density=True, color=colors[0], alpha=0.5 )
###plt.hist( df_TT.select("ll_pt").rdd.flatMap(lambda x: x).collect(), bins=binning, range=myrange, density=True, color=colors[1], alpha=0.5 )
###plt.legend(loc='upper right')
###plt.savefig('figures/h_ll_pt.png')

