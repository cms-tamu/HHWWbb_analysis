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

# Parameters and Selections
selection = "(isMuMu==0) OR (isElEl==0)"
genbb_selection = "((genjet1_partonFlavour == 5) AND (genjet2_partonFlavour == 5))"
gencc_selection = "((genjet1_partonFlavour == 4) AND (genjet2_partonFlavour == 4))"
sigSelection = "(" + genbb_selection + " OR " + gencc_selection + ")"
bkgSelection = "NOT" + sigSelection
now = datetime.datetime.now()
name_suffix = "miniAOD2RDD_" + str(getpass.getuser()) + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.hour) + "_" + str(now.minute) + "_" + str(now.second)
#Feature and samples ans weights
features = ["jet1_pt", "jet1_eta", "jet2_pt", "jet2_eta", "jj_pt", "ll_pt", "ll_eta", "llmetjj_DPhi_ll_met", "ht", "nJetsL"]
df_DYToLL_M10t50  = spark.read.load(sf.pathDYdf + "df_DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_Friend.parquet", format="parquet")
DYJetsToLL_M10t50 = sf.get_DYweights("DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8")
df_DYToLL_M50_0J  = spark.read.load(sf.pathDYdf + "df_DYToLL_0J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
DYJetsToLL_M50_0J = sf.get_DYweights("DYToLL_0J_13TeV-amcatnloFXFX-pythia8")
df_DYToLL_M50_1J  = spark.read.load(sf.pathDYdf + "df_DYToLL_1J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
DYJetsToLL_M50_1J = sf.get_DYweights("DYToLL_1J_13TeV-amcatnloFXFX-pythia8")
df_DYToLL_M50_2J  = spark.read.load(sf.pathDYdf + "df_DYToLL_2J_13TeV-amcatnloFXFX-pythia8_Friend.parquet", format="parquet")
DYJetsToLL_M50_2J = sf.get_DYweights("DYToLL_2J_13TeV-amcatnloFXFX-pythia8")
# Merge in a single DF and perform the selection
df_DY  = df_DYToLL_M10t50.union(df_DYToLL_M50_0J).union(df_DYToLL_M50_1J).union(df_DYToLL_M50_2J)
df_DY  = df_DY.where(selection)
# Add new column with the weight
def computeWeight(a, b):
    return a*b
weightUDF = udf(computeWeight, FloatType())
df_DY = df_DY.withColumn("weightExpr", weightUDF("event_reco_weight", "sample_weight"))
#Now define Signal and background 
df_DY_sig = df_DY.where(sigSelection)
df_DY_bac = df_DY.where(bkgSelection)

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

#trainMVA(bkgFiles, sigFiles, discriList, cut, weightExpr, MVAmethods, spectatorList, label, sigWeightExpr=sigSelection, bkgWeightExpr=bkgSelection, nSignal=1000000, nBkg=1000000)
#''' Train a MVA and write xml files for possibly different MVA methods (kBDT etc)'''
#MVA_fileName = "TMVA_" + name_suffix + ".root"
#file_MVA = R.TFile(MVA_fileName,"recreate")
#print "Will write MVA info in ", MVA_fileName 
#
#factory = R.TMVA.Factory(name_suffix, file_MVA)
#dataloader = R.TMVA.DataLoader("DYBDTTraining")
#
## This will be overridden if signal or background have specific weights
#
#for discriVar in discriList :
#    dataloader.AddVariable(discriVar)
#for spectatorVar in spectatorVariables :
#    dataloader.AddSpectator(spectatorVar)
#
#treename = "Friends"
#bkgChain = {}
#for bkg in bkgs.keys():
#    bkgChain[bkg] = R.TChain(treename)
#    for bkgFile in bkgs[bkg]["files"]:
#        bkgChain[bkg].Add(bkgFile)
#    dataloader.AddBackgroundTree(bkgChain[bkg], bkgs[bkg]["relativeWeight"])
#if bkgWeightExpr is not None:
#    dataloader.SetBackgroundWeightExpression("(({})*({}))".format(weightExpr, bkgWeightExpr))
#
#sigChain = {}
#for sig in sigs.keys():
#    sigChain[sig] = R.TChain(treename)
#    for sigFile in sigs[sig]["files"]:
#        sigChain[sig].Add(sigFile)
#    dataloader.AddSignalTree(sigChain[sig], sigs[sig]["relativeWeight"])
#if sigWeightExpr is not None:
#    dataloader.SetSignalWeightExpression("(({})*({}))".format(weightExpr, sigWeightExpr))
#
#dataloader.PrepareTrainingAndTestTree(R.TCut(trainCut), "nTrain_Signal={0}:nTest_Signal={0}:nTrain_Background={1}:nTest_Background={1}".format(nSignal, nBkg))
#for MVAmethod in MVAmethods:
#    factory.BookMethod(dataloader, getattr(R.TMVA.Types, MVAmethod), MVAmethod, "")
#
#factory.TrainAllMethods()
#factory.TestAllMethods()
#factory.EvaluateAllMethods()
#file_MVA.Close()
#
#MVA_out_in_tree(args):
#''' Merge MVA output(s) in trees '''
#try: 
#    inFileDir = args[0]
#    inFileName = args[1]
#    outFileDir = args[2]
#    list_dict_mva = args[3]
#except IndexError:
#    print "Error: args must be a tuple with [inFileDir, inFileName, outFileDir, list_dict_mva]"
#    return 
#inputFile = R.TFile.Open(os.path.join(inFileDir, inFileName))
#chain = inputFile.Get("t")
#tp_xs = inputFile.Get("cross_section").Clone()
#tp_wgt_sum = inputFile.Get("event_weight_sum").Clone()
#
#outFileName = os.path.join(outFileDir, inFileName)
#file_withBDTout = R.TFile(outFileName, "recreate")
#tree_withBDTout = chain.CloneTree(0)
#print "Merging MVA output in %s with %s entries."%(inFileName, chain.GetEntries())
#if chain.GetEntries() == 0:
#    print "Skip %s because 0 entries..."%inFileName
#    sys.exit()
#
## list all discriminative variables needed (removing overlap)
#fullDiscrList = []
#fullSpectList = []
#for dict_mva in list_dict_mva:
#    for var in dict_mva["discriList"]:
#        fullDiscrList.append(var)
#    for var in dict_mva["spectatorList"]:
#        fullSpectList.append(var)
#seen = set()
#discrList_unique = [ var for var in fullDiscrList if var not in seen and not seen.add(var) ] # allow preserving the order
#seen = set()
#spectList_unique = [ var for var in fullSpectList if var not in seen and not seen.add(var) ] # allow preserving the order
#
#BDT_out = {}
#reader = {}
#dict_variableName_Array = { variable: array('f', [0]) for variable in discrList_unique }
#dict_spectatorName_Array = { spectator: array('f', [0]) for spectator in spectList_unique }
#for dict_mva in list_dict_mva :
#    label = dict_mva["label"]
#    xmlFile = dict_mva["xmlFile"]
#    discriList = dict_mva["discriList"]
#    spectatorList = dict_mva["spectatorList"]
#    reader[label] = R.TMVA.Reader("Silent=1")
#    for var in discriList :
#        reader[label].AddVariable(var, dict_variableName_Array[var])
#    for var in spectatorList :
#        reader[label].AddSpectator(var, dict_spectatorName_Array[var])
#    leave_BDTout = "MVA_" + label
#    BDT_out[label] = array('d',[0])
#    tree_withBDTout.Branch(leave_BDTout, BDT_out[label], leave_BDTout + "/D")
#    reader[label].BookMVA(label, xmlFile)
#
#for entry in xrange(chain.GetEntries()):
#    chain.GetEntry(entry)
#    for dict_mva in list_dict_mva:
#        discriList = dict_mva["discriList"]
#        spectatorList = dict_mva["spectatorList"]
#        label = dict_mva["label"]
#        xmlFile = dict_mva["xmlFile"]
#        for var in discriList:
#            dict_variableName_Array[var][0] = getattr(chain, var)
#        for var in spectatorList:
#            dict_spectatorName_Array[var][0] = getattr(chain, var)
#        BDT_out[label][0] = reader[label].EvaluateMVA(label)
#    tree_withBDTout.Fill()
#
#print "Number of output tree entries: ", tree_withBDTout.GetEntries()
#tree_withBDTout.Write()
#tp_xs.Write()
#tp_wgt_sum.Write()
#file_withBDTout.Close()
#inputFile.Close()
#print "Output file : ", outFileName, " written.\n\n"

