# prepare_data.py

# pylint: disable-msg=w0614

from settings import *
from enums import *

from shutil import copyfile
from win32com import client
from math import isclose
from sqlalchemy import create_engine

mdd = client.Dispatch('MDM.Document')
ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = mroledb_connection

category_map = {}
adjusted_weight_targets = {}

def back_up():
    print('Backing up data...', end=' ')

    copyfile(src + '.mdd', dest + '.mdd')
    copyfile(src + '.ddf', dest + '.ddf')

    mdd.Open(dest + '.mdd')
    mdd.DataSources.Default.DBLocation = dest + '.ddf'
    mdd.Save()
    mdd.Close()
    print('OK')

def check_data():
    print('Checking data...', end=' ')

    # gets master brand list from db
    engine = create_engine(mssql_connection)
    master_brands = engine.execute('select mdm_category, mdm_list from brands').fetchall()
    brand_lists = set(brand['mdm_list'] for brand in master_brands)

    # get brands/lists from mdd
    mdd.Open(dest + '.mdd', mode=openConstants.oREAD)
    mdd_brands = {element.Name: lst for lst in brand_lists for element in mdd.Types(lst)}
    mdd.Close()

    # checks if master brand rotation corresponds to mdd brand rotation
    for brand in master_brands:
        master_list, mdm_list = brand['mdm_list'], mdd_brands[brand['mdm_category']]
        if master_list != mdm_list:
            raise Exception('Change in rotation for brand "{0}" (old: {1}, new: {2})'.format(
                        brand['mdm_category'], master_list, mdm_list
                    ))
    
    print('OK')

def add_weight_variable():
    print('Adding weight variable...', end=' ')

    mdd.Open(dest + '.mdd')
    if not mdd.Fields.Exist('weight'):
        wgt_var = mdd.CreateVariable('weight', 'Weight')
        wgt_var.DataType = DataTypeConstants.mtDouble
        wgt_var.UsageType = VariableUsageConstants.vtWeight
        mdd.Fields.Add(wgt_var)
    mdd.Save()
    mdd.Close()
    print('OK')

def read_category_map():
    print('Reading category map...', end=' ')
    mdd.Open(dest + '.mdd', mode=openConstants.oREAD)
    for i in range(mdd.CategoryMap.Count):
        category_map[mdd.CategoryMap.ItemValue(i)] = mdd.CategoryMap.ItemName(i)
    mdd.Close()
    print('{0} items'.format(len(category_map)))

def clean_data():

    ddf.Open()
    ddf.Execute("exec xp_syncdb")

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
 
    # testing weight normalization by overwriting c_15 with c_20
    _, rows_affected = ddf.Execute('update vdata set qage = {c_20} where qage = {c_15}')
    print('Testing weight normalization. {0} interview adjusted (c_15 > c_20)'.format(rows_affected))

    ddf.Close()

def get_frequences(var):
    ddf.Open()
    rs, _ = ddf.Execute('select {0}, count(*) as c from vdata group by {0}'.format(var))
    temp = {}
    while not rs.EOF:
        k, v = rs.Fields
        cat = ''.join(c for c in k.Value if c not in '{}') # stripping curly braces: {c_30} -> c_30
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
        
def weight_data():
    print('Weighting data...', end=' ')

    mdd.Open(dest + '.mdd')

    engine = client.Dispatch('mrWeight.WeightEngine')
    engine.Initialize(mdd)
    wgt = engine.CreateWeight('weight', ','.join(adjusted_weight_targets.keys()), wtMethod.wtRims)

    for k in adjusted_weight_targets.keys():
        rim = wgt.Rims[k]
        for i in range(rim.RimElements.Count):
            rim_element = rim.RimElements.Item(i)
            rim_element_category_name = category_map[rim_element.Category[0]]
            rim_element.Target = adjusted_weight_targets[k].get(rim_element_category_name, 0.0)

    wgt.TotalType = wtTotalType.wtUnweightedInput
    wgt.MaxWeight = 15

    engine.Prepare(wgt)
    engine.Execute(wgt)

    # delete engine object to flush ddf journal
    del engine

    mdd.Close()
    print('OK')

def main():
    back_up()
    check_data()
    add_weight_variable()
    read_category_map()
    clean_data()
    for k in initial_weight_targets.keys():
        print('Checking weight targets for {0}...'.format(k), end =" ")
        get_frequences(k)
        adjust_weight_targets(k)
    weight_data()
    print('Data preparation complete')

if __name__ == '__main__':
    main()