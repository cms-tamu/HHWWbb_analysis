import os, sys, math, shutil, datetime, getpass
import pandas as pd
import numpy as np
from scipy import signal
from scipy.optimize import *
from scipy.fftpack import *
import matplotlib
import matplotlib.pyplot as plt
from root_numpy import root2array
import Useful_func as uf

ROOTfiles=["crab_Run2016H-07Aug17-v1_v2_FINAL.root"]
m_min, m_max, m_step = 0.2113,  9., 0.03995
#my_selec='massC>0.2113 && massF>0.2113 && massC<9. && massF<9.'
# signal
#Data_diagonal                = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events', selection="abs(massC-massF) <= (0.13 + 0.065*(massC+massF)/2.) && massC > 0.25 && massC < 9. && massF > 0.25 && massF < 9.")#, branches=my_branches, selection=my_selec)
#Data_control_offDiagonal     = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events', selection="abs(massC-massF) > (0.13 + 0.065*(massC+massF)/2.) && massC > 0.25 && massC < 9. && massF > 0.25 && massF < 9.")
#Data_control_Iso_offDiagonal = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events', selection="isoC_1mm >= 0 && isoC_1mm < 2. && isoF_1mm >= 0 && isoF_1mm < 2. && abs(massC-massF) > (0.13 + 0.065*(massC+massF)/2.) && massC > 0.25 && massC < 9. && massF > 0.25 && massF < 9.")
#Data_control_nonIso          = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events', selection="isoC_1mm > 2. && isoC_1mm < 8. && isoF_1mm > 2. && isoF_1mm < 8. && massC > 0.25 && massC < 9. && massF > 0.25 && massF < 9.")
#Data_signal                  = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events', selection="isoC_1mm>=0 && isoC_1mm<2. && isoF_1mm>=0 && isoF_1mm<2. && abs(massC-massF) <= (0.13 + 0.065*(massC+massF)/2.) && massC > 0.25 && massC < 9. && massF > 0.25 && massF < 9.")
# bb 
Data_bbS17 = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events_orphan', selection="orph_dimu_isoTk < 2 && orph_dimu_isoTk >= 0 && ((orph_PtMu0>17 && TMath::Abs(orph_EtaMu0<0.9)) || (orph_PtMu1>17 && TMath::Abs(orph_EtaMu1<0.9))) && orph_dimu_mass > 0.2113 && orph_dimu_mass < 9.")
Data_bbSmx = uf.root2panda(ROOTfiles[0], 'cutFlowAnalyzerPXBL2PXFL2_Data/Events_orphan', selection="orph_dimu_isoTk < 2 && orph_dimu_isoTk >= 0 && orph_PtOrph>17 && TMath::Abs(orph_EtaOrph)<0.9 && orph_dimu_mass > 0.2113 && orph_dimu_mass < 9.")
# Plot S17 and SMIX
Data_bbS17.orph_dimu_mass.plot(kind='hist', title="S17", bins=np.arange(m_min,m_max,m_step), grid=True)
plt.xlabel('Mass [GeV]')
plt.ylabel('Events')
plt.savefig('figures/S17.png')
plt.clf()
Data_bbSmx.orph_dimu_mass.plot(kind='hist', title="SMIX", bins=np.arange(m_min,m_max,m_step), grid=True)
plt.xlabel('Mass [GeV]')
plt.ylabel('Events')
plt.savefig('figures/SMIX.png')
plt.clf()

#****************************************************************************
#                         Create template for S17
#****************************************************************************
data = Data_bbS17.orph_dimu_mass
hist, bin_edges = np.histogram(data, bins=np.arange(m_min, m_max, m_step))
bin_centres = (bin_edges[:-1] + bin_edges[1:])/2
# initial guess for the fitting coefficients
p_gs         = [400., 0.00, 2.260] # N, mu, sigma
p_gs_d       = [100., -2.0, 1.000]
p_gs_u       = [9999, 5.00, 5.000]
#p_gs         = [1290, 129, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1] # N, mu, sigma
#p_gs_d       = [50, 1, 0., 0., 0., 0., 0., 0.]
#p_gs_u       = [10000, 300, 1., 1., 1., 1., 1., 1.]
p_exp_mumu   = [50.0, 3.50, 10.00] # N, tau, Const
p_exp_mumu_d = [0.00, 0.00, 0.000]
p_exp_mumu_u = [9000, 10.0, 100.0]
p_exp_mumu   = [400., 0.05, 0.010] # N, mumuP, mumuC
p_exp_mumu_d = [0.00, 0.00, 0.000]
p_exp_mumu_u = [9000, 1.50, 0.030]
p_gs_ah      = [370., 0.27, 0.084] # N, mu, sigma # Ad-HOC
p_gs_ah_d    = [10.0, 0.00, 0.010]
p_gs_ah_u    = [9000, 0.40, 0.100]
p_gs_eta     = [10.0, 0.55, 0.033] # N, mu, sigma # ETA
p_gs_eta_d   = [0.00, 0.54, 0.010]
p_gs_eta_u   = [9000, 0.56, 0.040]
p_gs_rho     = [10.0, 0.78, 0.036] # N, mu, sigma # RHO
p_gs_rho_d   = [0.00, 0.77, 0.010]
p_gs_rho_u   = [9000, 0.79, 0.043]
p_gs_phi     = [150., 1.02, 0.012] # N, mu, sigma # PHI
p_gs_phi_d   = [10.0, 1.01, 0.002]
p_gs_phi_u   = [9000, 1.03, 0.047]
p_CB_Jpsi    = [3000., 1.7, 3.09, 0.035] # N, a, mean, sigma (n=2 hardcoded) # JPSI
p_CB_Jpsi_d  = [1000., 1.3, 3.07, 0.020]
p_CB_Jpsi_u  = [9999, 1.8, 3.12, 0.040]
p_gs_psi     = [180., 3.69, 0.020] # N, mu, sigma # PSI
p_gs_psi_d   = [50.0, 3.60, 0.015]
p_gs_psi_u   = [3000, 3.80, 0.043]
p_tot   = p_gs   + p_exp_mumu   + p_exp_mumu   + p_gs_ah   + p_gs_eta   + p_gs_rho   + p_gs_phi   + p_CB_Jpsi   + p_gs_psi
p_tut_d = p_gs_d + p_exp_mumu_d + p_exp_mumu_d + p_gs_ah_d + p_gs_eta_d + p_gs_rho_d + p_gs_phi_d + p_CB_Jpsi_d + p_gs_psi_d
p_tut_u = p_gs_u + p_exp_mumu_u + p_exp_mumu_u + p_gs_ah_u + p_gs_eta_u + p_gs_rho_u + p_gs_phi_u + p_CB_Jpsi_u + p_gs_psi_u

coeff, var_matrix = curve_fit(uf.myPDF, bin_centres, hist, p0=p_tot, bounds=(p_tut_d, p_tut_u))
hist_fit = uf.myPDF(bin_centres, *coeff)
print '-----Printing parameter of the S17 fit-----'
print coeff
print '----------'
#plt.hist(data, label='Test data', bins=np.arange(m_min, m_max, m_step))
fig = plt.figure(1)
gs  = matplotlib.gridspec.GridSpec(2,1, height_ratios=[4,1], hspace=0.0)
ax0 = fig.add_subplot(gs[0])
S17 = ax0.errorbar(bin_centres, hist, yerr = np.sqrt(hist), marker = '|', linewidth=0.5, color='k', label='Test data', alpha=0.9, zorder=2)
FIT = ax0.plot(bin_centres, hist_fit, linewidth=1, color='r', label='Fitted data', alpha=1, zorder=1)
plt.legend((S17[0], FIT[0]), ('Data', 'FIT'))
plt.title('S17')
plt.xticks(np.arange(0., m_max, 0.5))
plt.xlabel('Mass [GeV]')
plt.ylabel('Events')
plt.grid(True)
ax1 = fig.add_subplot(gs[1],sharex=ax0)
ax1.errorbar(bin_centres, hist/hist_fit, yerr = ((hist/hist_fit)*np.sqrt(hist)/hist), marker = '|', linewidth=0.5, color='k', label='Test data', alpha=1)
plt.ylim(0.5, 1.5)
plt.yticks(np.arange(0.5, 1.5, 0.25))
plt.grid(True)
plt.savefig('figures/S17_fit.png', dpi = 300)
plt.clf()

#****************************************************************************
#                         Create template for SMix
#****************************************************************************
dataMix = Data_bbSmx.orph_dimu_mass
hist, bin_edges = np.histogram(dataMix, bins=np.arange(m_min, m_max, m_step))
bin_centres = (bin_edges[:-1] + bin_edges[1:])/2
# initial guess for the fitting coefficients
p_gs         = [400., 0.00, 2.260] # N, mu, sigma
p_gs_d       = [100., -2.0, 1.000]
p_gs_u       = [9999, 5.00, 5.000]
#p_gs         = [1290, 129, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1] # N, mu, sigma
#p_gs_d       = [50, 1, 0., 0., 0., 0., 0., 0.]
#p_gs_u       = [10000, 300, 1., 1., 1., 1., 1., 1.]
p_exp_mumu   = [50.0, 3.50, 10.00] # N, tau, Const
p_exp_mumu_d = [0.00, 0.00, 0.000]
p_exp_mumu_u = [9000, 10.0, 100.0]
p_exp_mumu   = [400., 0.05, 0.010] # N, mumuP, mumuC
p_exp_mumu_d = [0.00, 0.00, 0.000]
p_exp_mumu_u = [9000, 1.50, 0.030]
p_gs_ah      = [370., 0.27, 0.084] # N, mu, sigma # Ad-HOC
p_gs_ah_d    = [10.0, 0.00, 0.010]
p_gs_ah_u    = [9000, 0.40, 0.100]
p_gs_eta     = [10.0, 0.55, 0.033] # N, mu, sigma # ETA
p_gs_eta_d   = [0.00, 0.54, 0.010]
p_gs_eta_u   = [9000, 0.56, 0.040]
p_gs_rho     = [10.0, 0.78, 0.036] # N, mu, sigma # RHO
p_gs_rho_d   = [0.00, 0.77, 0.010]
p_gs_rho_u   = [9000, 0.79, 0.043]
p_gs_phi     = [150., 1.02, 0.012] # N, mu, sigma # PHI
p_gs_phi_d   = [10.0, 1.01, 0.002]
p_gs_phi_u   = [9000, 1.03, 0.047]
p_CB_Jpsi    = [3000., 1.7, 3.09, 0.035] # N, a, mean, sigma (n=2 hardcoded) # JPSI
p_CB_Jpsi_d  = [1000., 1.3, 3.07, 0.020]
p_CB_Jpsi_u  = [9999, 1.8, 3.12, 0.040]
p_gs_psi     = [180., 3.69, 0.020] # N, mu, sigma # PSI
p_gs_psi_d   = [50.0, 3.60, 0.015]
p_gs_psi_u   = [3000, 3.80, 0.043]
p_tot   = p_gs   + p_exp_mumu   + p_exp_mumu   + p_gs_ah   + p_gs_eta   + p_gs_rho   + p_gs_phi   + p_CB_Jpsi   + p_gs_psi
p_tut_d = p_gs_d + p_exp_mumu_d + p_exp_mumu_d + p_gs_ah_d + p_gs_eta_d + p_gs_rho_d + p_gs_phi_d + p_CB_Jpsi_d + p_gs_psi_d
p_tut_u = p_gs_u + p_exp_mumu_u + p_exp_mumu_u + p_gs_ah_u + p_gs_eta_u + p_gs_rho_u + p_gs_phi_u + p_CB_Jpsi_u + p_gs_psi_u

coeffMix, var_matrix = curve_fit(uf.myPDF, bin_centres, hist, p0=p_tot, bounds=(p_tut_d, p_tut_u))
hist_fit = uf.myPDF(bin_centres, *coeffMix)
print '-----Printing parameter of the SMix fit-----'
print coeffMix
print '----------'
fig = plt.figure(1)
gs  = matplotlib.gridspec.GridSpec(2,1, height_ratios=[4,1], hspace=0.0)
ax0 = fig.add_subplot(gs[0])
S17 = ax0.errorbar(bin_centres, hist, yerr = np.sqrt(hist), marker = '|', linewidth=0.5, color='k', label='Test data', alpha=0.9, zorder=2)
FIT = ax0.plot(bin_centres, hist_fit, linewidth=1, color='r', label='Fitted data', alpha=1, zorder=1)
plt.title('SMix')
plt.legend((S17[0], FIT[0]), ('Data', 'FIT'))
plt.xticks(np.arange(0., m_max, 0.5))
plt.xlabel('Mass [GeV]')
plt.ylabel('Events')
plt.grid(True)
ax1 = fig.add_subplot(gs[1],sharex=ax0)
ax1.errorbar(bin_centres, hist/hist_fit, yerr = ((hist/hist_fit)*np.sqrt(hist)/hist), marker = '|', linewidth=0.5, color='k', label='Test data', alpha=1)
plt.ylim(0.5, 1.5)
plt.yticks(np.arange(0.5, 1.5, 0.25))
plt.grid(True)
plt.savefig('figures/SMix_fit.png', dpi = 300)
plt.clf()

# Now you should save teh coefficients so that you can build your template
