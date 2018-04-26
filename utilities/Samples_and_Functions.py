import ROOT
from ROOT import TObject, TH1F, TH1D

#ROOT File folder
pathROOT   = "/data/taohuang/20180412_HHbbWW_addHME_10k_final/"
#After miniAOD2RDD
pathCSV1   = "/data/RDD/miniAOD2RDD/miniAOD2RDD_lpernie_2018_4_20_13_21_8/"
#DY estimation
pathDYroot    = "/data/taohuang/HHNtuple_20180418_DYestimation"
pathDYdf      = "/data/RDD/DYminiAOD2RDD/miniAOD2RDD_lpernie_2018_4_21_18_35_33/"
DYfeatures    = ['jet1_pt', 'jet1_eta', 'jet2_pt', 'jet2_eta', 'jj_pt', 'll_pt', 'll_eta', 'llmetjj_DPhi_ll_met', 'ht', 'nJetsL']
DYneeded_vars = ['genjet1_partonFlavour','genjet2_partonFlavour','cross_section','event_weight_sum','sample_weight','event_reco_weight']
#General
RDDpath    = "/data/RDD/"
FIGpath    = "figures/"

#DY Functions
def get_DYweights( samplename ):
    sampleinfo = {}
    tfile = ROOT.TFile( pathDYroot + "/" + samplename+"_Friend.root" ,"READ")
    h_cutflow = tfile.Get("h_cutflow")
    sampleinfo["cross_section"] = tfile.Get("cross_section").GetVal()
    sampleinfo["event_weight_sum"] = h_cutflow.GetBinContent(1)
    sampleinfo["relativeWeight"] = sampleinfo["cross_section"]/h_cutflow.GetBinContent(1)
    return sampleinfo
