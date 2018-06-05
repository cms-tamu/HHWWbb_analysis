import glob
from   numpy.lib.recfunctions import stack_arrays
from   root_numpy import root2array 
import pandas as pd
import numpy as np
from   scipy.interpolate import BPoly
import math
def root2panda(files_path, tree_name, **kwargs):
    '''
    Args:
    -----
        files_path: a string like './data/*.root', for example
        tree_name: a string like 'Collection_Tree' corresponding to the name of the folder inside the root 
                   file that we want to open
        kwargs: arguments taken by root2array, such as branches to consider, start, stop, step, etc
    Returns:
    --------    
        output_panda: a pandas dataframe like allbkg_df in which all the info from the root file will be stored
    
    Note:
    -----
        if you are working with .root files that contain different branches, you might have to mask your data
        in that case, return pd.DataFrame(ss.data)
    '''
    # -- create list of .root files to process
    files = glob.glob(files_path)
    
    # -- process ntuples into rec arrays
    ss = stack_arrays([root2array(fpath, tree_name, **kwargs) for fpath in files])

    try:
        return pd.DataFrame(ss)
    except Exception:
        return pd.DataFrame(ss.data)

def flatten(column):
    '''
    Args:
    -----
        column: a column of a pandas df whose entries are lists (or regular entries -- in which case nothing is done)
                e.g.: my_df['some_variable'] 
    Returns:
    --------    
        flattened out version of the column. 
        For example, it will turn:
        [1791, 2719, 1891]
        [1717, 1, 0, 171, 9181, 537, 12]
        [82, 11]
        ...
        into:
        1791, 2719, 1891, 1717, 1, 0, 171, 9181, 537, 12, 82, 11, ...
    '''
    try:
        return np.array([v for e in column for v in e])
    except (TypeError, ValueError):
        return column

def create_stream(df, num_obj, sort_col):
   
    n_variables = df.shape[1]
    var_names = df.keys()

    data = np.zeros((df.shape[0], num_obj, n_variables), dtype='float32')

    # -- call functions to build X (a.k.a. data)                                                                                                                                                                      
    sort_objects(df, data, sort_col, num_obj)
    
    # -- ix_{train, test} from above or from previously stored ordering
    Xobj_train = data[ix_train]
    Xobj_test = data[ix_test]
    
    #print 'Scaling features ...'
    scale(Xobj_train, var_names, savevars=True) # scale training sample and save scaling
    scale(Xobj_test, var_names, savevars=False) # apply scaling to test set
    return Xobj_train, Xobj_test

def sort_objects(df, data, SORT_COL, max_nobj):
    ''' 
    sort objects using your preferred variable
    
    Args:
    -----
        df: a dataframe with event-level structure where each event is described by a sequence of jets, muons, etc.
        data: an array of shape (nb_events, nb_particles, nb_features)
        SORT_COL: a string representing the column to sort the objects by
        max_nobj: number of particles to cut off at. if >, truncate, else, -999 pad
    
    Returns:
    --------
        modifies @a data in place. Pads with -999
    
    '''
    #!import tqdm
    # i = event number, event = all the variables for that event 
    #!for i, event in tqdm.tqdm(df.iterrows(), total=df.shape[0]): 
    for i, event in df.iterrows():

        # objs = [[pt's], [eta's], ...] of particles for each event 
        objs = np.array(
                [v.tolist() for v in event.get_values()], 
                dtype='float32'
            )[:, (np.argsort(event[SORT_COL]))[::-1]]

        # total number of tracks per jet      
        nobjs = objs.shape[1] 

        # take all tracks unless there are more than n_tracks 
        data[i, :(min(nobjs, max_nobj)), :] = objs.T[:(min(nobjs, max_nobj)), :] 

        # default value for missing tracks 
        data[i, (min(nobjs, max_nobj)):, :  ] = -999

def scale(data, var_names, savevars, VAR_FILE_NAME='scaling.json'):
    ''' 
    Args:
    -----
        data: a numpy array of shape (nb_events, nb_particles, n_variables)
        var_names: list of keys to be used for the model
        savevars: bool -- True for training, False for testing
                  it decides whether we want to fit on data to find mean and std 
                  or if we want to use those stored in the json file 
    
    Returns:
    --------
        modifies data in place, writes out scaling dictionary
    '''
    import json
    
    scale = {}
    if savevars: 
        for v, name in enumerate(var_names):
            #print 'Scaling feature %s of %s (%s).' % (v + 1, len(var_names), name)
            f = data[:, :, v]
            slc = f[f != -999]
            m, s = slc.mean(), slc.std()
            slc -= m
            slc /= s
            data[:, :, v][f != -999] = slc.astype('float32')
            scale[name] = {'mean' : float(m), 'sd' : float(s)}
            
        with open(VAR_FILE_NAME, 'wb') as varfile:
            json.dump(scale, varfile)

    else:
        with open(VAR_FILE_NAME, 'rb') as varfile:
            varinfo = json.load(varfile)

        for v, name in enumerate(var_names):
            #print 'Scaling feature %s of %s (%s).' % (v + 1, len(var_names), name)
            f = data[:, :, v]
            slc = f[f != -999]
            m = varinfo[name]['mean']
            s = varinfo[name]['sd']
            slc -= m
            slc /= s
            data[:, :, v][f != -999] = slc.astype('float32')

def feature_selection(train_data, features, k):
    """
    Definition:
    -----------
            !! ONLY USED FOR INTUITION, IT'S USING A LINEAR MODEL TO DETERMINE IMPORTANCE !!
            Gives an approximate ranking of variable importance and prints out the top k
    Args:
    -----
            train_data = dictionary containing keys "X" and "y" for the training set, where:
                X = ndarray of dim (# training examples, # features)
                y = array of dim (# training examples) with target values
            features = names of features used for training in the order in which they were inserted into X
            k = int, the function will print the top k features in order of importance
    """

    # -- Select the k top features, as ranked using ANOVA F-score
    tf = SelectKBest(score_func=f_classif, k=k)
    tf.fit_transform(train_data["X"], train_data["y"])

    # -- Return names of top features
    logging.getLogger("process_data").info("The {} most important features are {}".format(k, [f for (_, f) in sorted(zip(tf.scores_, features), reverse=True)][:k]))

def my_min(a, b):
  if(a<b): return a
  else: return b
def my_max(a, b):
  if(a>b): return a
  else: return b

class PDF(object):
  def __init__(self, pdf, size=(200,200)):
    self.pdf = pdf
    self.size = size

  def _repr_html_(self):
    return '<iframe src={0} width={1[0]} height={1[1]}></iframe>'.format(self.pdf, self.size)

  def _repr_latex_(self):
    return r'\includegraphics[width=1.0\textwidth]{{{0}}}'.format(self.pdf)

def CrisBall(x, *p):
  """ A Gaussian curve on one side and a power-law on the other side. Used in
  physics to model lossy processes.
  See http://en.wikipedia.org/wiki/Crystal_Ball_function
  Note that the definition used here differs slightly. At the time of this
  writing, the wiki article has some discrepancies in definitions/plots. This
  definition makes it easier to fit the function by using complex numbers
  and by negating any negative values for a and n.
  This version of the crystal ball is normalized by an additional parameter.
  params: N, a, n, xb, sig
  """
  x = x+0j # Prevent warnings...
  N, a, n, xb, sig = p
  if a < 0:
    a = -a
  if n < 0:
    n = -n
  aa = abs(a)
  A = (n/aa)**n * np.exp(- aa**2 / 2)
  B = n/aa - aa
  total = 0.*x
  total += ((x-xb)/sig  > -a) * N * np.exp(- (x-xb)**2/(2.*sig**2))
  total += ((x-xb)/sig <= -a) * N * A * (B - (x-xb)/sig)**(-n)
  try:
    return total.real
  except:
    return total
  return total

def gauss(x, *p):
    A, mu, sigma = p
    return A*np.exp(-(x-mu)**2/(2.*sigma**2))

def binCoeff(n,k):
  return math.factorial(n)/(math.factorial(k)*math.factorial(n-k))

def myPDF(x, *p):
  # Parameters
  #beN, be0, be1, be2, be3, be4, be5, be6,
  gsN, gsMu, gsSig,  exp_lonN, exp_lonT, exp_lonC,   exp_mumuN, exp_mumuP, exp_mumuC,     gsN_ah, gsMu_ah, gsSig_ah,    gsN_eta, gsMu_eta, gsSig_eta,    gsN_rho, gsMu_rho, gsSig_rho,   gsN_phi, gsMu_phi, gsSig_phi,    cbN, cba, cbMu, cbSig,    gsN_psi, gsMu_psi, gsSig_psi = p
  #General shape
  general   =  gsN*np.exp(-(x-gsMu)**2/(2.*gsSig**2)) #np.poly1d([gsN, gsMu, gsSig, g4])#gsN*np.exp(-(x-gsMu)**2/(2.*gsSig**2))
  #Exp long
  exp_long  = exp_lonN* np.exp(-x*exp_lonT) + exp_lonC
  #Exp mumu
  exp_mumu  = exp_mumuN*(x*pow( (x/0.2113)*(x/0.2113) - 1.0, exp_mumuP )*np.exp( -exp_mumuC*( (x/0.2113)*(x/0.2113) - 1.0 ) ))
  #Ad-hoc
  gauss_ah  = gsN_ah*np.exp(-(x-gsMu_ah)**2/(2.*gsSig_ah**2))
  #Eta
  gauss_eta = gsN_eta*np.exp(-(x-gsMu_eta)**2/(2.*gsSig_eta**2)) 
  #Rho
  gauss_rho = gsN_rho*np.exp(-(x-gsMu_rho)**2/(2.*gsSig_rho**2)) 
  #Phi
  gauss_phi = gsN_phi*np.exp(-(x-gsMu_phi)**2/(2.*gsSig_phi**2)) 
  #JPSI
  cbn = 2
  A = (cbn/abs(cba))**cbn * np.exp(- abs(cba)**2 / 2)
  B = cbn/abs(cba) - abs(cba)
  cb_JPsi   = ((x-cbMu)/cbSig  > -cba) * cbN * np.exp(- (x-cbMu)**2/(2.*cbSig**2)) + ((x-cbMu)/cbSig <= -cba) * cbN * A * (B - (x-cbMu)/cbSig)**(-cbn) 
  #psi
  gauss_psi = gsN_psi*np.exp(-(x-gsMu_psi)**2/(2.*gsSig_psi**2)) 

  return general + exp_long + exp_mumu + gauss_ah + gauss_eta + gauss_rho + gauss_phi + cb_JPsi + gauss_psi

