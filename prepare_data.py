# prepare_data.py

# pylint: disable-msg=w0614

import settings
from settings import initial_weight_targets, mssql_connection, \
    mroledb_connection, mdd_path, ddf_path
from enums import *
from math import isclose
from win32com import client
from sqlalchemy import create_engine

adjusted_weight_targets = {}

mssql_engine = create_engine(mssql_connection)

mdd = client.Dispatch('MDM.Document')
ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = mroledb_connection

def check_data():
    print('Checking data...', end=' ')

    # gets master brand list from db
    
    master_brands = mssql_engine.execute('select mdm_category, mdm_list from brands').fetchall()
    brand_lists = set(brand[1] for brand in master_brands)

    # get brands/lists from mdd
    mdd.Open(mdd_path, mode=openConstants.oREAD)
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
            raise Exception('Change in rotation for brand "{0}" (old: {1}, new: {2})'.format(
                        brand['mdm_category'], master_list, mdm_list
                    ))
    
    print('OK')
    
def clean_data():

    ddf.Open()

    # basco checks
    print('Cleaning data...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where not comp.ContainsAny({comp, comp_sc})')
    print('{0} incompletes removed'.format(rows_affected))

    print('Cleaning data...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where DataCollection.Status.ContainsAny({Test})')
    print('{0} test interviews removed'.format(rows_affected))
    
    print('Cleaning data...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where f5g.AnswerCount() = 0')
    print('{0} interviews with f5g = 0 removed'.format(rows_affected))
    
    print('Cleaning data...', end=' ')
    _, rows_affected = ddf.Execute('delete from vdata where cdouble(intend-intstart)*60*24 <= 5')
    print('{0} interviews with interview duration <= 5 minutes removed'.format(rows_affected))
 
    ddf.Close()

def get_frequences(var):
    ddf.Open()
    rs, _ = ddf.Execute('select {0}, count(*) as c from vdata group by {0}'.format(var))
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
    adjusted_weight_targets[var] = {k: initial_weight_targets[var][k] for k in adjusted_weight_targets[var].keys()}

    # normalizing intial weight if sum of all targets is less than 100
    normalization_factor = 100 / sum(adjusted_weight_targets[var].values())
    if isclose(normalization_factor, 1.0, abs_tol=0.0):
        print('OK')
    else:
        print('Categories missing. Increasing {0} factors by {1:.1%}'.format(var, normalization_factor - 1))
        adjusted_weight_targets[var] = {k: v * normalization_factor for k, v in adjusted_weight_targets[var].items()}

def add_weight_variable():
    print('Adding weight variable...', end=' ')

    mdd.Open(mdd_path)
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

    mdd.Open(mdd_path)

    weight_engine = client.Dispatch('mrWeight.WeightEngine')
    weight_engine.Initialize(mdd)
    wgt = weight_engine.CreateWeight('weight', ','.join(adjusted_weight_targets.keys()), wtMethod.wtRims)

    for k in adjusted_weight_targets.keys():
        rim = wgt.Rims[k]
        for i in range(rim.RimElements.Count):
            rim_element = rim.RimElements.Item(i)
            rim_element_category_name = settings.category_map[rim_element.Category[0]]
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
    check_data()
    clean_data()
    for k in initial_weight_targets.keys():
        print('Checking weight targets for {0}...'.format(k), end =" ")
        get_frequences(k)
        adjust_weight_targets(k)
    add_weight_variable()
    weight_data()
    print('Data preparation complete\n')

if __name__ == '__main__':
    import initialize
    initialize.main()
    main()