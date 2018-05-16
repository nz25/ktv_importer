from settings import *
from shutil import copyfile
from win32com import client
from math import isclose

mdd = client.Dispatch('MDM.Document')
ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = '''
    Provider=mrOleDB.Provider.2;
    Data Source=mrDataFileDsc;
    Location=''' + dest + '''.ddf;
    Initial Catalog=''' + dest + '''.mdd;
    MR Init MDM Access=1;
    MR Init Category Names=1;'''

category_values = {}
category_names = {}
adjusted_weight_targets = {}

def back_up():
    print('Backing up data...')

    copyfile(src + '.mdd', dest + '.mdd')
    copyfile(src + '.ddf', dest + '.ddf')

    mdd.Open(dest + '.mdd')
    mdd.DataSources.Default.DBLocation = dest + '.ddf'
    mdd.Save()
    mdd.Close()

def add_weight_variable():
    print('Adding weight variable...')

    # adding weight variable
    mdd.Open(dest + '.mdd')
    if not mdd.Fields.Exist('weight'):
        wgt_var = mdd.CreateVariable('weight', 'Weight')
        wgt_var.DataType = 6 # DataTypeConstants.mtDouble
        wgt_var.UsageType = 8192 # VariableUsageConstants.vtWeight
        mdd.Fields.Add(wgt_var)
    mdd.Save()
    mdd.Close()

def init_category_maps():
    print('Intializing category map...')
    mdd.Open(dest + '.mdd')
    for i in range(mdd.CategoryMap.Count):
        category_values[mdd.CategoryMap.ItemValue(i)] = mdd.CategoryMap.ItemName(i)
    global category_names
    category_names = {v: k for k, v in category_values.items()}
    mdd.Close()

# 2. Cleaning data
def clean_data():
    print('Cleaning data...')

    ddf.Open()
    ddf.Execute("exec xp_syncdb")

    #Removing incompletes
    _, rows_affected = ddf.Execute('delete from vdata where not comp.ContainsAny({comp, comp_sc})')
    print('    {0} incompletes removed'.format(rows_affected))

    #Removing tests
    _, rows_affected = ddf.Execute('delete from vdata where DataCollection.Status.ContainsAny({Test})')
    print('    {0} test interviews removed'.format(rows_affected))

    #Removing basco knock-out (f5g)
    _, rows_affected = ddf.Execute('delete from vdata where f5g.AnswerCount() = 0')
    print('    {0} interviews with f5g = 0 removed'.format(rows_affected))

    #Removing basco knock-out (interview duration)
    _, rows_affected = ddf.Execute('delete from vdata where cdouble(intend-intstart)*60*24 <= 5')
    print('    {0} interviews with interview duration <= 5 minutes removed'.format(rows_affected))

    # testing weight normalization
    # rs, rows_affected = ddf.Execute('update vdata set qage = {c_20} where qage = {c_15}')
    # print('Testing weight normalization. {0} interview adjusted (c_15 > c_20)'.format(rows_affected))

    ddf.Close()


#Checking distribution of weight variables
def get_frequences(var):
    print('    Getting frequencies')
    ddf.Open()
    rs, _ = ddf.Execute('select {0}, count(*) as c from vdata group by {0}'.format(var))
    temp = {}
    while not rs.EOF:
        k, v = rs.Fields
        if v.Value > 0:
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
        print('    No weight normalization necessary')
    else:
        print('    Normalizing {0} by {1}'.format(var, normalization_factor))
        adjusted_weight_targets[var] = {k: v * normalization_factor for k, v in adjusted_weight_targets[var].items()}

def weight_data():
    print('Weighting data...')

    mdd.Open(dest + '.mdd')

    engine = client.Dispatch('mrWeight.WeightEngine')
    engine.Initialize(mdd)
    wgt = engine.CreateWeight('weight', ','.join(adjusted_weight_targets.keys()), 3) #3 = wtMethod.wtRims

    for k in adjusted_weight_targets.keys():
        rim = wgt.Rims[k]
        for i in range(rim.RimElements.Count):
            elem = rim.RimElements.Item(i)
            elem.Target = adjusted_weight_targets[k][category_values[elem.Category[0]]]

    wgt.TotalType = 2 #2 = wtTotalType.wtUnweightedInput
    wgt.MaxWeight = 15

    engine.Prepare(wgt)
    engine.Execute(wgt)

    del engine

    mdd.Close()

# Workflow
back_up()
add_weight_variable()
init_category_maps()
clean_data()
for k in initial_weight_targets.keys():
    print('Adjusting weight targets for {0}'.format(k))
    get_frequences(k)
    adjust_weight_targets(k)
weight_data()
