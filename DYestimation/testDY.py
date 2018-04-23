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

# Load what you have trained
#X_train = pd.read_csv('models/X_train.csv')
X_test = pd.read_csv('models/X_test.csv')
#y_train = pd.read_csv('models/y_train.csv')
y_test = pd.read_csv('models/y_test.csv')
#print list(y_test.columns.values)
model = joblib.load('models/DYmodel.pkl') 
#Predict
y_rf = model.predict_proba(X_test) #[prob_back, prob_sig]
# Make preobbaility between -0.5 and 0.5
ProbS = y_rf[:,1]
ProbS = ProbS-0.5

#Plots
ProbS.loc[y_test['0'] == 1]

plt.hist(ProbS)
plt.ylabel('Probability')
plt.show()
#plt.scatter(ProbS, X_train2[:, 1])



