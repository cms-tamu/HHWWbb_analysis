# Import stuff
import os, sys, shutil, datetime, getpass
import Samples_and_FunctionsDY as sf
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

# Creating a dictionary with all files
FolderList = []
for FoldName in os.listdir(sf.pathDYroot):
    FolderList.append(FoldName)
File_dic = dict()
for iFold in FolderList:
    fileList = []
    for iFile in os.listdir(sf.pathDYroot+"/"+iFold):
        fileList.append(sf.pathDYroot + "/" + iFold + "/" + iFile)
    File_dic[iFold] = fileList

# THIS GIVE ERROR
df = spark.read.format("org.dianahep.sparkroot").load("/data/taohuang/HHNtuple_20180418_DYestimation/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/6214A145-5711-E811-997E-0CC47A78A42C_Friend.root")
# THIS WORK
#df = spark.read.format("org.dianahep.sparkroot").load("/data/taohuang/20180412_HHbbWW_addHME_10k_final/WWToLNuQQ_aTGC_13TeV-madgraph-pythia8_final.root")

#dfs=[]
#df_result=[]
#for key in File_dic:
#    print "Folder", key
#    print '  Files', File_dic[key]
#    for iFile in File_dic[key]:
#      df = spark.read.format("org.dianahep.sparkroot").load("/data/taohuang/HHNtuple_20180418_DYestimation//DoubleMuonRun2016Bver2/386897D3-530C-E811-9E5F-001E675A69DC_Friend.root")
#      dfs.append(df)
#    df_result.append(pd.concat(dfs))
#    dfs=[]

# Converting Root files into a DATAFRAME (Very useful, checnl root2panda in Useful_func.py)

#
#def get_sample(inFileDir, samplename):
#    sampleinfo = {}
#    sampleinfo["files"] = [ os.path.join(inFileDir, samplename+"_Friend.root") ]
#    #allsamplenames = [x.split('/')[1] for x in Samplelist.datasets]
#    #index = allsamplenames.index(samplename)
#    #sampleinfo["cross_section"] = Samplelist.MCxsections[index]
#    tfile = ROOT.TFile(sampleinfo["files"][0] ,"READ")
#    h_cutflow = tfile.Get("h_cutflow")
#    sampleinfo["cross_section"] = tfile.Get("cross_section").GetVal()
#    sampleinfo["event_weight_sum"] = h_cutflow.GetBinContent(1)
#    sampleinfo["relativeWeight"] = sampleinfo["cross_section"]/h_cutflow.GetBinContent(1)
#    return sampleinfo
#
#date = "2017_02_17"
#suffix = "bb_cc_vs_rest_10var"
#label_template = "DATE_BDTDY_SUFFIX"
#
#inFileDir = "/fdata/hepx/store/user/taohuang/HHNtuple_20180405_DYestimation/"
#
#
#DYJetsToLL_M10to50_db = get_sample(inFileDir, "DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8")
#DYJetsToLL_M50_0J_db = get_sample(inFileDir, "DYToLL_0J_13TeV-amcatnloFXFX-pythia8")
#DYJetsToLL_M50_1J_db = get_sample(inFileDir, "DYToLL_1J_13TeV-amcatnloFXFX-pythia8")
#DYJetsToLL_M50_2J_db = get_sample(inFileDir, "DYToLL_2J_13TeV-amcatnloFXFX-pythia8")
#
#bkgFiles = { 
#        "DYJetsToLL_M-10to50": { 
#	           "files": DYJetsToLL_M10to50_db["files"],
#		   "relativeWeight": DYJetsToLL_M10to50_db["relativeWeight"]
#                },
#        "DYJetsToLL_M-50_0J": {
#	           "files": DYJetsToLL_M50_0J_db["files"],
#		   "relativeWeight": DYJetsToLL_M50_0J_db["relativeWeight"]
#                },
#        "DYJetsToLL_M-50_1J": {
#	           "files": DYJetsToLL_M50_1J_db["files"],
#		   "relativeWeight": DYJetsToLL_M50_1J_db["relativeWeight"]
#                },
#        }
#
#print "bgfiles  ",bkgFiles
#
#discriList = [
#        "jet1_pt",
#        "jet1_eta",
#        "jet2_pt",
#        "jet2_eta",
#        "jj_pt",
#        "ll_pt",
#        "ll_eta",
#        "llmetjj_DPhi_ll_met",
#        "ht",
#        "nJetsL"
#        ]
#
#spectatorList = []
#cut = "(isMuMu || isElEl)"
#MVAmethods = ["kBDT"]
#weightExpr = "event_reco_weight * sample_weight"
#
#sigFiles = copy.deepcopy(bkgFiles)
#genbb_selection = "genjet1_partonFlavour == 5 && genjet2_partonFlavour == 5"
#gencc_selection = "genjet1_partonFlavour == 4 && genjet2_partonFlavour == 4"
#sigSelection = "((%s) || (%s))"%(genbb_selection, gencc_selection)
#bkgSelection = "(! %s)"%(sigSelection)
#
#label = label_template.replace("DATE", date).replace("SUFFIX", suffix)
#
#if __name__ == "__main__":
#    trainMVA(bkgFiles, sigFiles, discriList, cut, weightExpr, MVAmethods, spectatorList, label, sigWeightExpr=sigSelection, bkgWeightExpr=bkgSelection, nSignal=1000000, nBkg=1000000)
