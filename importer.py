from settings import *
from shutil import copyfile
from win32com import client

# 1. Backing up orignal files
print('Backing up data...')

copyfile(src + '.mdd', dest + '.mdd')
copyfile(src + '.ddf', dest + '.ddf')

mdd = client.Dispatch('MDM.Document')
mdd.Open(dest + '.mdd')
mdd.DataSources.Default.DBLocation = dest + '.ddf'

# adding weight variable
if not mdd.Fields.Exist('weight'):
    wgt_var = mdd.CreateVariable('weight', 'Weight')
    wgt_var.DataType = 6 # DataTypeConstants.mtDouble
    wgt_var.UsageType = 8192 # VariableUsageConstants.vtWeight
    mdd.Fields.Add(wgt_var)

category_values = {}
for i in range(mdd.CategoryMap.Count):
    category_values[mdd.CategoryMap.ItemValue(i)] = mdd.CategoryMap.ItemName(i)
category_names = {v: k for k, v in category_values.items()}

mdd.Save()
mdd.Close()

# 2. Cleaning data
print('Cleaning data...')

ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = '''
    Provider=mrOleDB.Provider.2;
    Data Source=mrDataFileDsc;
    Location=''' + dest + '''.ddf;
    Initial Catalog=''' + dest + '''.mdd;
    MR Init MDM Access=1;
    MR Init Category Names=1;'''
ddf.Open()
ddf.Execute("exec xp_syncdb")

#Removing incompletes
rs, rows_affected = ddf.Execute('delete from vdata where not comp.ContainsAny({comp, comp_sc})')
print('{0} incompletes removed'.format(rows_affected))

#Removing tests
rs, rows_affected = ddf.Execute('delete from vdata where DataCollection.Status.ContainsAny({Test})')
print('{0} test interviews removed'.format(rows_affected))

#Removing basco knock-out (f5g)
rs, rows_affected = ddf.Execute('delete from vdata where f5g.AnswerCount() = 0')
print('{0} interviews with f5g = 0 removed'.format(rows_affected))

#Removing basco knock-out (interview duration)
rs, rows_affected = ddf.Execute('delete from vdata where cdouble(intend-intstart)*60*24 <= 5')
print('{0} interviews with interview duration <= 5 minutes removed'.format(rows_affected))

# testing weight normalization
# rs, rows_affected = ddf.Execute('update vdata set qage = {c_20} where qage = {c_15}')
# print('Testing weight normalization. {0} interview adjusted (c_15 > c_20)'.format(rows_affected))


#Checking distribution of weight variables
rs, _ = ddf.Execute('select qage, count(*) as c from vdata group by qage')
qage = {}
while not rs.EOF:
    k, v = rs.Fields
    if v.Value > 0:
        cat = ''.join(c for c in k.Value if c not in '{}')
        qage[cat] = v.Value
    rs.MoveNext()

rs, _ = ddf.Execute('select fa, count(*) as c from vdata group by fa')
fa = {}
while not rs.EOF:
    k, v = rs.Fields
    if v.Value > 0:
        cat = ''.join(c for c in k.Value if c not in '{}')
        fa[cat] = v.Value
    rs.MoveNext()

rs, _ = ddf.Execute('select fj, count(*) as c from vdata group by fj')
fj = {}
while not rs.EOF:
    k, v = rs.Fields
    if v.Value > 0:
        cat = ''.join(c for c in k.Value if c not in '{}')
        fj[cat] = v.Value
    rs.MoveNext()

ddf.Close()

#3. Normalizing weights
print('Normalizing weight targets based on collected data...')

qage = {k: qage_targets[k] for k in qage.keys()}
qage = {k: v * 100 / sum(qage.values()) for k, v in qage.items()}

fa = {k: fa_targets[k] for k in fa.keys()}
fa = {k: v * 100 / sum(fa.values()) for k, v in fa.items()}

fj = {k: fj_targets[k] for k in fj.keys()}
fj = {k: v * 100 / sum(fj.values()) for k, v in fj.items()}

#4. Weighting data
print('Weighting data...')

mdd.Open(dest + '.mdd')

engine = client.Dispatch('mrWeight.WeightEngine')
engine.Initialize(mdd)
wgt = engine.CreateWeight('weight', 'qage, fa, fj', 3) #3 = wtMethod.wtRims

rim = wgt.Rims['qage']
for i in range(rim.RimElements.Count):
    elem = rim.RimElements.Item(i)
    elem.Target = qage[category_values[elem.Category[0]]]

rim = wgt.Rims['fa']
for i in range(rim.RimElements.Count):
    elem = rim.RimElements.Item(i)
    elem.Target = fa[category_values[elem.Category[0]]]

rim = wgt.Rims['fj']
for i in range(rim.RimElements.Count):
    elem = rim.RimElements.Item(i)
    elem.Target = fj[category_values[elem.Category[0]]]

wgt.TotalType = 2 #2 = wtTotalType.wtUnweightedInput
wgt.MaxWeight = 15

engine.Prepare(wgt)
engine.Execute(wgt)

del engine

mdd.Close()

from sqlalchemy import create_engine
engine = create_engine('mssql+pyodbc://AVANUSQL702/dw_04_dashboard?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
print(engine)