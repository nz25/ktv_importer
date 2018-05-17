# files paths
src = r'D:\ktv\KTVONLINE_18'
dest = r'D:\ktv\KTVONLINE_18_IMPORT'

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

# connection strings
mssql_connection = 'mssql+pyodbc://AVANUSQL702/dw_04_live?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes'
ddf_connection = '''
    Provider=mrOleDB.Provider.2;
    Data Source=mrDataFileDsc;
    Location={0}.ddf;
    Initial Catalog={0}.mdd;
    MR Init MDM Access=1;
    MR Init Category Names=1;'''.format(dest)
pyodbc_connection = 'Driver={SQL Server};Server=avanusql702;Database=dw_temp;Trusted_Connection=yes;'