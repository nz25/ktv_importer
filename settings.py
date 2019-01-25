from datetime import date

# files paths

# dynamic discovery (below) of the wave did not work on 2.7 as Juli wave has not started yet
# it can be activated again as soon automatic execution process will ensure that the final
# data for the previous wave has been loaded
# WAVE = date.today().strftime('%y%m')

WAVE = '1901'
RAW_DATA_LOCATION = f'\\\\avanufil002\\IAV_internal\\Dashboard BVR\\KTV_2015\\LiveDaten\\KTVONLINE_{WAVE}'
RAW_MDD_PATH = f'{RAW_DATA_LOCATION}.mdd'
RAW_DDF_PATH = f'{RAW_DATA_LOCATION}.ddf'

DP_DATA_LOCATION = f'\\\\avanufil002\\ActiveProjects\\KTV\\07_Data\\01_Data Processing\\TOM\\Online\\20{WAVE[:2]}-{WAVE[2:]}\\Data\\Live-Data\\KTVONLINE_{WAVE}'
DP_MDD_PATH = f'{DP_DATA_LOCATION}.mdd'
DP_DDF_PATH = f'{DP_DATA_LOCATION}.ddf'

PROCESSED_DATA_LOCATION = f'{RAW_DATA_LOCATION}_IMPORT'
MDD_PATH = f'{PROCESSED_DATA_LOCATION}.mdd'
DDF_PATH = f'{PROCESSED_DATA_LOCATION}.ddf'

DP_LOCATION = f'\\\\avanufil002\\ActiveProjects\\KTV\\07_Data\\01_Data Processing\\TOM\\Online\\20{WAVE[:2]}-{WAVE[2:]}\\Data\\Live-Data\\KTVONLINE_{WAVE}'

# weight targets
INITIAL_WEIGHT_TARGETS = {
    'qage': {'c_15': 5.89, 'c_20': 17.34, 'c_30': 17.59,
             'c_40': 20.02, 'c_50': 22.59, 'c_60': 16.57},
    'fa': {'mann': 49.00, 'frau': 51.00},
    'fj': {'c_1': 13.24, 'c_2': 15.63, 'c_3': 4.28, 'c_4': 3.02,
           'c_5': 0.82, 'c_6': 2.18, 'c_7': 7.52,  'c_8': 1.96,
           'c_9': 9.65, 'c_10': 21.74, 'c_11': 4.93, 'c_12': 1.21,
           'c_13': 4.97, 'c_14': 2.73, 'c_15': 3.48, 'c_16': 2.64}
}

# database connection strings
MSSQL_CONNECTION = 'mssql+pyodbc://ZorenkoD_CS:Wuerfel1234@avanusql702/dw_04_live?driver=SQL+Server+Native+Client+11.0'
SQLITE_CONNECTION = f'{PROCESSED_DATA_LOCATION}.ddf'
MROLEDB_CONNECTION = f'''
    Provider=mrOleDB.Provider.2;
    Data Source=mrDataFileDsc;
    Location={PROCESSED_DATA_LOCATION}.ddf;
    Initial Catalog={PROCESSED_DATA_LOCATION}.mdd;
    MR Init MDM Access=1;
    MR Init Category Names=1;'''

# shared variables
CATEGORY_MAP = {}
SERIAL_INCREMENT = 0
SERIAL_CRITERIA = ''

# open_ends
LEVENSHTEIN_CUTOFF = 0.95
BIGRAMMS = ['bc','bf','bj','bx','cb','cf','cg','cj','cq','cv','cx','cy','dq','dx','fc','fj','fq','fx','fy','gc','gq','gx','gy','hx','jb','jc','jd','jg','jh','ji','jj','jk','jl','jm','jn','jp','jq','jr','js','jt','jv','jw','jx','jy','jz','kj','kx','lq','mq','mx','nx','oq','pj','px','qa','qb','qc','qd','qe','qf','qg','qh','qj','qk','qm','qp','qq','qr','qs','qt','qv','qw','qx','qy','qz','rq','rx','sq','sx','tx','uj','uy','vb','vc','vf','vg','vj','vn','vq','vx','vy','wq','wx','wy','wz','xb','xc','xd','xf','xg','xh','xj','xk','xl','xm','xn','xq','xr','xs','xv','xw','xy','xz','yf','yg','yh','yi','yj','yk','yq','yt','yu','yv','yx','yy','yz','zf','zh','zj','zr','zx','zy','aj','bd','bh','bn','by','dj','eq','fh','fk','fp','fv','fw','gj','hc','hj','ij','kg','kh','km','kn','kz','lj','oj','uu','uz','wj','ws','xt','ys','zd','zg','zm','zz','cd','dh','fb','fn','gf','hf','jo','kd','lh','mj','sj','tj','ui','uw','aw','bg','df','dz','ej','fg','gb','gg','gh','gw','gz','hh','hk','hz','iu','pd']
REGEX_CRITERIA = r'[\d]+[a-zA-ZöüäÖÜÄ]+[\d]'
NEURONET_PATH = 'neuronet_2018-06-21.pkl'
NEURONET_CUTOFF = 0.97

# outputs
DAU_LOCATION = f'\\\\avanufil002\\ActiveProjects\\KTV\\06_Coding\\02_Coding Files\\DAUs\\ktv_{WAVE}.dau'
CFILE_LOCATION = f'\\\\avanufil002\\ActiveProjects\\KTV\\06_Coding\\02_Coding Files\\DAUs\\dw_ktv_cfile_20{WAVE[:2]}-{WAVE[2:]}.txt'