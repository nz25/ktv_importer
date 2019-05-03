# prepare_data.py

# pylint: disable-msg=w0614

import settings
from settings import INITIAL_WEIGHT_TARGETS, MSSQL_CONNECTION, \
    MROLEDB_CONNECTION, MDD_PATH, DDF_PATH, START_DATE, END_DATE
from enums import *
from math import isclose
from win32com import client
from sqlalchemy import create_engine

adjusted_weight_targets = {}

mssql_engine = create_engine(MSSQL_CONNECTION)

mdd = client.Dispatch('MDM.Document')
ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = MROLEDB_CONNECTION

def check_data():
    print('Checking data...', end=' ')

    # gets master brand list from db
    
    master_brands = mssql_engine.execute('select mdm_category, mdm_list from brands').fetchall()
    brand_lists = set(brand[1] for brand in master_brands)

    # get brands/lists from mdd
    mdd.Open(MDD_PATH, mode=openConstants.oREAD)
    mdd_brands = {element.Name: lst for lst in brand_lists for element in mdd.Types(lst)}
    mdd_brands = {}
    for lst in brand_lists:
        for element in mdd.Types(lst):
            mdd_brands[element.Name] = lst

    mdd.Close()

    # checks if master brand rotation corresponds to mdd brand rotation
    for brand in master_brands:
        master_list, mdm_list = brand['mdm_list'], mdd_brands[brand['mdm_category']]
        if master_list != mdm_list:
            raise Exception(f'''Change in rotation for brand '{brand['mdm_category']}':
            (old: {master_list}, new: {mdm_list})''')
    
    print('OK')
    
def clean_data():

    ddf.Open()

    # basco checks
    print('Cleaning incompletes...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where not comp.ContainsAny({comp, comp_sc})')
    print(f'{rows_affected} removed')

    print('Cleaning test interviews...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where DataCollection.Status.ContainsAny({Test})')
    print(f'{rows_affected} removed')
    
    print('Cleaning sum check interviews...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where f5g.AnswerCount() = 0')
    print(f'{rows_affected} removed')
    
    print('Cleaning speedsters...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where cdouble(intend-intstart)*60*24 <= 5')
    print(f'{rows_affected} removed')
        
    print('Cleaning other wave...', end=' ')
    date_format = '%d.%m.%Y %H:%M:%S'
    _, rows_affected = ddf.Execute(f"""delete
        from vdata
        where not (intend > '{START_DATE.strftime(date_format)}'
            and intend < '{END_DATE.strftime(date_format)}')""")
    print(f'{rows_affected} removed')

    print('Counting interviews...', end=' ')
    rs, _ = ddf.Execute('select count(*) as c from hdata')
    if rs.EOF:
        raise Exception('Empty dataset after cleaning. Exiting...')
    else:
        row_count = rs.Fields['c'].Value
        rs.Close()
        print(f'{row_count} remaining')
    
    ddf.Close()

def get_frequences(var):
    ddf.Open()
    rs, _ = ddf.Execute(f'select {var}, count(*) as c from vdata group by {var}')
    temp = {}
    while not rs.EOF:
        k, v = rs.Fields
        cat = k.Value[1:-1]
        temp[cat] = v.Value
        rs.MoveNext()
    adjusted_weight_targets[var] = temp
    ddf.Close()

def adjust_weight_targets(var):

    # putting initial weight target in adjusted targets dictionary
    adjusted_weight_targets[var] = {k: INITIAL_WEIGHT_TARGETS[var][k] for k in adjusted_weight_targets[var].keys()}

    # normalizing intial weight if sum of all targets is less than 100
    normalization_factor = 100 / sum(adjusted_weight_targets[var].values())
    if isclose(normalization_factor, 1.0, abs_tol=0.0):
        print('OK')
    else:
        print('Categories missing. Increasing {0} factors by {1:.1%}'.format(var, normalization_factor - 1))
        adjusted_weight_targets[var] = {k: v * normalization_factor for k, v in adjusted_weight_targets[var].items()}

def add_weight_variable():
    print('Adding weight variable...', end=' ')

    mdd.Open(MDD_PATH)
    if not mdd.Fields.Exist('weight'):
        wgt_var = mdd.CreateVariable('weight', 'Weight')
        wgt_var.DataType = DataTypeConstants.mtDouble
        wgt_var.UsageType = VariableUsageConstants.vtWeight
        mdd.Fields.Add(wgt_var)
    mdd.Save()
    mdd.Close()

    ddf.Open()
    ddf.Execute('exec xp_syncdb')
    ddf.Close()
    print('OK')
  
def weight_data():
    print('Weighting data...', end=' ')

    mdd.Open(MDD_PATH)

    weight_engine = client.Dispatch('mrWeight.WeightEngine')
    weight_engine.Initialize(mdd)
    wgt = weight_engine.CreateWeight('weight', ','.join(adjusted_weight_targets.keys()), wtMethod.wtRims)

    for k in adjusted_weight_targets.keys():
        rim = wgt.Rims[k]
        for i in range(rim.RimElements.Count):
            rim_element = rim.RimElements.Item(i)
            rim_element_category_name = settings.CATEGORY_MAP[rim_element.Category[0]]
            rim_element.Target = adjusted_weight_targets[k].get(rim_element_category_name, 0.0)

    wgt.TotalType = wtTotalType.wtUnweightedInput
    wgt.MaxWeight = 15

    weight_engine.Prepare(wgt)
    weight_engine.Execute(wgt)

    # delete engine object to flush ddf journal
    del weight_engine

    mdd.Close()
    print('OK')

def main():
    print('DATA PREPARATION')
    check_data()
    clean_data()
    for k in INITIAL_WEIGHT_TARGETS.keys():
        print(f'Checking weight targets for {k}...', end =" ")
        get_frequences(k)
        adjust_weight_targets(k)
    add_weight_variable()
    weight_data()
    print('Data preparation complete', end='\n\n')

if __name__ == '__main__':
    import initialize
    initialize.main()
    main()