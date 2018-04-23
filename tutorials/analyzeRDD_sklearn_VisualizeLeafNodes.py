# Import stuff
import os, sys, math, shutil, datetime, getpass
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utilities.Samples_and_Functions as sf
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
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

print "This script produce a plot of the decision surface for Random Forrest algorithms. To have a 2D plot I only train using 2 features."
#Parameters
Selection = 'll_M > 76.'
features  = ["hme_mean_reco","llmetjj_DPhi_ll_jj","jj_pt", "ll_M"] # 4 Feaures in total
combos = [[0, 1], [0, 2], [2, 3]] # I select here the 2 feature to use. I can make several combinations
FilesToConsider=["df_TTTo2L2Nu_TuneCUETP8M2_ttHtranche3_13TeV-powheg-pythia8_final.root.csv",
                 "df_GluGluToRadionToHHTo2B2VTo2L2Nu_M-500_narrow_13TeV-madgraph-v2_final.root.csv"]
cmap = plt.cm.RdYlBu
plot_step = 0.08  # fine step width for decision surface contours
plot_step_coarser = 0.5  # step widths for coarse classifier guesses

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

mod_name = ['DecisionTreeClassifier','RandomForestClassifier','AdaBoostClassifier']
models = [DecisionTreeClassifier(max_depth=None, criterion="entropy",splitter='best',class_weight={0: 0.7, 1: 0.3}),
          RandomForestClassifier(n_estimators=25, max_features="auto", max_depth=None, random_state=2, bootstrap=True, n_jobs=-1),
          AdaBoostClassifier(DecisionTreeClassifier(max_depth=None, criterion="entropy"), n_estimators=50)]
if len(mod_name) != len(models):
    print "Exiting... mod_name and models have a different size!" 
    sys.quit()

plot_idx = 1
for pair in combos:
    for model in models:
        print "check 0"
        #Select only the 2 feaures
        X_train2 = X_train[:, pair]
        #Train
        model.fit(X_train2, y_train)   
        scores = model.score(X_train2, y_train)
        # Create a title for each column 
        model_title = str(type(model)).split(".")[-1][:-2][:-len("Classifier")]
        model_details = model_title
        if hasattr(model, "estimators_"):
            model_details += " with {} estimators".format(len(model.estimators_))
        print(model_details + " with features", pair,"has a score of", scores)

        # Create a Canvas for 3x3 plots, and add a title
        plt.subplot(len(combos), len(models), plot_idx)
        if plot_idx <= len(models):
            plt.title(model_title)

        # Now plot the decision boundary using a fine mesh as input to a filled contour plot
        x_min, x_max = X_train2[:, 0].min() - 1, X_train2[:, 0].max() + 1
        y_min, y_max = X_train2[:, 1].min() - 1, X_train2[:, 1].max() + 1
        xx, yy = np.meshgrid(np.arange(x_min, x_max, plot_step), np.arange(y_min, y_max, plot_step))

        # Plot either a single DecisionTreeClassifier or alpha blend the decision surfaces of the ensemble of classifiers
        print "check 1"
        if isinstance(model, DecisionTreeClassifier):
            Z = model.predict(np.c_[xx.ravel(), yy.ravel()])
            Z = Z.reshape(xx.shape)
            cs = plt.contourf(xx, yy, Z, cmap=cmap)
        else:
            # Choose alpha blend level with respect to the number of estimators that are in use (noting that AdaBoost can use fewer estimators
            # than its maximum if it achieves a good enough fit early on)
            estimator_alpha = 1.0 / len(model.estimators_)
            for tree in model.estimators_:
                Z = tree.predict(np.c_[xx.ravel(), yy.ravel()])
                Z = Z.reshape(xx.shape)
                cs = plt.contourf(xx, yy, Z, alpha=estimator_alpha, cmap=cmap)

        # Build a coarser grid to plot a set of ensemble classifications to show how these are different to what we see in the decision surfaces.
        # These points are regularly space and do not have a black outline
        print "check 2"
        xx_coarser, yy_coarser = np.meshgrid( np.arange(x_min, x_max, plot_step_coarser), np.arange(y_min, y_max, plot_step_coarser))
        Z_points_coarser = model.predict(np.c_[xx_coarser.ravel(), yy_coarser.ravel()] ).reshape(xx_coarser.shape)
        cs_points = plt.scatter(xx_coarser, yy_coarser, s=15, c=Z_points_coarser, cmap=cmap, edgecolors="none")

        # Plot the training points, these are clustered together and have a black outline
        print "check 3"
        plt.scatter(X_train2[:, 0], X_train2[:, 1], c=y_train, cmap=ListedColormap(['r', 'y', 'b']), edgecolor='k', s=20)
        plot_idx += 1  # move on to the next plot in sequence
        print "check 4"

print "Final Plot"
plt.suptitle("Classifiers on feature subsets of the Iris dataset")
plt.axis("tight")
plt.savefig('figures/VisualizeLeafNodes.png')
