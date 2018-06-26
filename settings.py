from datetime import date

# files paths
wave = date.today().strftime('%y%m')
raw_data_location = f'D:\\ktv\\KTVONLINE_{wave}'
raw_mdd_path = f'{raw_data_location}.mdd'
raw_ddf_path = f'{raw_data_location}.ddf'
processed_data_location = f'{raw_data_location}_IMPORT'
mdd_path = f'{processed_data_location}.mdd'
ddf_path = f'{processed_data_location}.ddf'

# weight targets
initial_weight_targets = {
    'qage': {'c_15': 5.89, 'c_20': 17.34, 'c_30': 17.59,
             'c_40': 20.02, 'c_50': 22.59, 'c_60': 16.57},
    'fa': {'mann': 49.00, 'frau': 51.00},
    'fj': {'c_1': 13.24, 'c_2': 15.63, 'c_3': 4.28, 'c_4': 3.02,
           'c_5': 0.82, 'c_6': 2.18, 'c_7': 7.52,  'c_8': 1.96,
           'c_9': 9.65, 'c_10': 21.74, 'c_11': 4.93, 'c_12': 1.21,
           'c_13': 4.97, 'c_14': 2.73, 'c_15': 3.48, 'c_16': 2.64}
}

# database connection strings
mssql_connection = 'mssql+pyodbc://./dw_04_live?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes'
sqlite_connection = f'{processed_data_location}.ddf'
mroledb_connection = f'''
    Provider=mrOleDB.Provider.2;
    Data Source=mrDataFileDsc;
    Location={processed_data_location}.ddf;
    Initial Catalog={processed_data_location}.mdd;
    MR Init MDM Access=1;
    MR Init Category Names=1;'''

# shared variables
category_map = {}
serial_increment = 0
serial_criteria = ''

# open_ends
LEVENSHTEIN_CUTOFF = 0.95
bigramms = ['bc', 'bf', 'bj', 'bx', 'cb', 'cf', 'cg', 'cj', 'cq', 'cv', 'cx', 'cy', 'dq', 'dx', 'fc', 'fj', 'fq', 'fx', 'fy', 'gc', 'gq', 'gx', 'gy', 'hx', 'jb', 'jc', 'jd', 'jg', 'jh', 'ji', 'jj', 'jk', 'jl', 'jm', 'jn', 'jp', 'jq', 'jr', 'js', 'jt', 'jv', 'jw', 'jx', 'jy', 'jz', 'kj', 'kx', 'lq', 'mq', 'mx', 'nx', 'oq', 'pj', 'px', 'qa', 'qb', 'qc', 'qd', 'qe', 'qf', 'qg', 'qh', 'qj', 'qk', 'qm', 'qp', 'qq', 'qr', 'qs', 'qt', 'qv', 'qw', 'qx', 'qy', 'qz', 'rq', 'rx', 'sq', 'sx', 'tx', 'uj', 'uy', 'vb', 'vc', 'vf', 'vg', 'vj', 'vn', 'vq', 'vx', 'vy', 'wq', 'wx', 'wy', 'wz', 'xb', 'xc', 'xd', 'xf', 'xg', 'xh', 'xj', 'xk', 'xl', 'xm', 'xn', 'xq', 'xr', 'xs', 'xv', 'xw', 'xy', 'xz', 'yf', 'yg', 'yh', 'yi', 'yj', 'yk', 'yq', 'yt', 'yu', 'yv', 'yx', 'yy', 'yz', 'zf', 'zh', 'zj', 'zr', 'zx', 'zy', 'aj', 'bd', 'bh', 'bn', 'by', 'dj', 'eq', 'fh', 'fk', 'fp', 'fv', 'fw', 'gj', 'hc', 'hj', 'ij', 'kg', 'kh', 'km', 'kn', 'kz', 'lj', 'oj', 'uu', 'uz', 'wj', 'ws', 'xt', 'ys', 'zd', 'zg', 'zm', 'zz', 'cd', 'dh', 'fb', 'fn', 'gf', 'hf', 'jo', 'kd', 'lh', 'mj', 'sj', 'tj', 'ui', 'uw', 'aw', 'bg', 'df', 'dz', 'ej', 'fg', 'gb', 'gg', 'gh', 'gw', 'gz', 'hh', 'hk', 'hz', 'iu', 'pd']