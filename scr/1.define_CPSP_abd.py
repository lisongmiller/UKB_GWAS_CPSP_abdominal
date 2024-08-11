##################################################
## This script is to 1)extract HR operation date, 2)extract GP operation date, 3)define CPSP phenotype
##################################################
## {License_info}
##################################################
## Author: Song Li
## Copyright: Copyright {year}, {project_name}
## Credits: [{credit_list}]
## License: {license}
## Version: {mayor}.{minor}.{rel}
## Mmaintainer: {maintainer}
## Email: {contact_email}
## Status: {dev_status}
##################################################


import os
import pandas as pd
import numpy as np
import datetime
os.chdir('/home/songli/PhD_project/UKB_CPSP_abd/scr')

###Part A: extract operation date
##A1: HR date
#import HR data, which alrady subset by abd operation
hr = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/raw_abd_operation/abd_operation_HR.txt.gz',sep='\t',dtype=str,index_col=0)

#OPCS4 CODE LIST
opcs4_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/opcs4.txt',sep='\t',names=['codes'],dtype=str)

#Making a mask to match operation with date by instance and array
opr_mask = hr.columns[hr.columns.str.contains('41272')]
date_opr_mask = hr.columns[hr.columns.str.contains('41282')]
mask_value = np.array(-hr[opr_mask].isin(list(opcs4_code.codes))) #operation in OPCS4 will be marked as false

#Select all operation date columns
hr_abd_date_temp = hr[date_opr_mask].mask(mask_value) #maks function replace value when it is true, so when creating mask label abd operation as false

#Define date conversion function from SAS date to %Y%m%d
def sas_date(indate):
    ymddate = pd.to_timedelta(indate, unit='D') + pd.Timestamp('1960-1-1')
    ymddate = ymddate.astype(str)
    return ymddate

hr_abd_date_temp2 = hr_abd_date_temp.fillna(0).apply(pd.to_numeric)
hr_abd_date_temp3 = hr_abd_date_temp2.apply(lambda x: sas_date(x))
hr_abd_date_temp4 = hr_abd_date_temp3.mask(mask_value)

hr_operation_dates = pd.DataFrame(hr_abd_date_temp4.apply(lambda x: '/'.join(x[-x.isna()]), axis=1),columns = ['operation_dates']) #MERGING IN ONE COLUMN THE DATES #DataFrame.apply(,axis indicate application of function to each row.)
hr_operation_dates.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_dates.txt',sep='\t',index=True,header=True)


#extract abd operation records and merge file with abd operation date
hr_abd_ope = hr[opr_mask].mask(mask_value)
hr_abd_ope['eid'] = hr_abd_ope.index
var_list = list(hr_abd_ope.columns)
hr_abd_ope = hr_abd_ope.melt(id_vars=['eid'], value_vars = var_list,var_name = 's_41272_var',value_name='s_41272')
hr_abd_ope = hr_abd_ope.drop('s_41272_var',axis=1)

hr_abd_date_temp5 = hr_abd_date_temp4
hr_abd_date_temp5['eid'] = hr_abd_date_temp5.index
var_list = list(hr_abd_date_temp5.columns)
hr_abd_date_temp5 = hr_abd_date_temp5.melt(id_vars=['eid'], value_vars = var_list,var_name = 's_41282_var',value_name='s_41282')
hr_abd_date_temp5 = hr_abd_date_temp5.drop('s_41282_var',axis=1)

hr_abd_ope_date = pd.merge(hr_abd_ope, hr_abd_date_temp5, left_index=True, right_index=True)
hr_abd_ope_date = hr_abd_ope_date.dropna(subset=['s_41272'])
hr_abd_ope_date = hr_abd_ope_date.drop('eid_y',axis=1)
hr_abd_ope_date = hr_abd_ope_date.rename(columns={"eid_x": "eid"})
hr_abd_ope_date.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_codes_dates.txt',sep='\t',index=False,header=True)

##remove patients with operation data before date cut-off
hr_abd_ope_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_codes_dates.txt',sep='\t',dtype=str)
hr_abd_ope_date['event_dt_time'] = pd.to_datetime(hr_abd_ope_date.s_41282) #No missing data for this
date_cutoff = pd.to_datetime('1997-01-01')
hr_abd_ope_date.loc[(hr_abd_ope_date['event_dt_time'] < date_cutoff),'invalid_date']= 'invalid'
hr_operations_invalidlist = hr_abd_ope_date.loc[hr_abd_ope_date.invalid_date.isin(['invalid']),'eid']
hr_operations_invalidlist = hr_operations_invalidlist.drop_duplicates()
hr_operations_invalidlist.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc_invalid_hrdate.txt.gz',sep='\t',index = False)



##A2: GP date
#Import GP operation data, remove missing date record
gp_operations = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/raw_abd_operation/abd_operation_GP.txt.gz',sep='\t',dtype=str,usecols= [0,2])
gp_operations = gp_operations.dropna(subset = ['event_dt'])
gp_operations = gp_operations.set_index('eid',drop=False)

#Since HR data is more reliable than GP data, we only use GP data when subjects without HR data; Save subjects only with operation in GP
hr_abd_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_codes_dates.txt',sep='\t',dtype=str)
gp_operations = gp_operations[~gp_operations.eid.isin(hr_abd_date.eid)]

#QC remove eid with invalid date
gp_operations['event_dt_time'] = pd.to_datetime(gp_operations.event_dt, format='%d/%m/%Y')

date_cutoff = pd.to_datetime('1997-01-01') ### temporarily set natural cut-off of GP operation data, this will also remove wid with invalid date in if event_dt not in ('01/Jan/1901'd, '02/Feb/1902'd, '03/Mar/1903'd);
# next line of code also remove eid with future operation records
gp_operations.loc[ (gp_operations['event_dt_time'] < date_cutoff) |
                   (gp_operations['event_dt_time'] == pd.to_datetime('2037-07-07')), 'invalid_date'] = 'invalid'
gp_operations_invalidlist = gp_operations.loc[gp_operations.invalid_date.isin(['invalid'])].index
gp_operations = gp_operations.drop(labels = gp_operations_invalidlist,axis = 'index')
gp_operations = gp_operations[~gp_operations.index.isin(gp_operations_invalidlist)].drop(labels = ['event_dt_time','invalid_date'],axis = 'columns')
gp_operations['dt'] = pd.to_datetime(gp_operations.event_dt,format="%d/%m/%Y")
gp_operations['event_dt'] = gp_operations.dt.astype(str)
gp_operations = gp_operations.drop('dt',axis = 'columns')

##merge hr and gp subjects with invalid operation date
hr_operations_invalidlist = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc_invalid_hrdate.txt.gz',sep='\t',dtype=str)
gp_operations_invalidlist = pd.DataFrame(gp_operations_invalidlist)
qc_invalid_date = pd.concat([hr_operations_invalidlist,gp_operations_invalidlist])
qc_invalid_date.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc_invalid_date.txt.gz',sep='\t',index = False)


#prepare abd date, eid with duplicated records or one operation record are dealt separately
dup_index_list = gp_operations.index[gp_operations.index.duplicated(keep=False)] # #mark all duplicated eid index as TRUE, and extract all duplicated eid index
dup_index_list = dup_index_list.drop_duplicates()
dup_gp_opdate =  gp_operations.loc[dup_index_list].apply(lambda x: '/'.join(gp_operations.loc[x[0],'event_dt']), axis = 1) #select by index, then extract by index then join
dup_gp_opdate = dup_gp_opdate.drop_duplicates()
dup_gp_opdate_df = dup_gp_opdate.to_frame(name="event_dt")
dup_gp_opdate_df['eid'] = dup_gp_opdate_df.index
uni_index_list = gp_operations.index[~gp_operations.index.isin(dup_index_list)]
uni_gp_opdate =  gp_operations.loc[uni_index_list]
gp_abd_date = pd.concat([uni_gp_opdate,dup_gp_opdate_df]) # concatenate temp with chem by row
gp_abd_date.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/gpponly_operations_eid.txt',sep='\t',index=False,header=True)



###Part B: extract prescription date
##B1: GP1 analgesic prescription records
GP1_scr_read2na = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/5.GP_data/gp1_scripts_read2na.txt.gz',sep='\t',dtype=str)
GP1_scr_read2 = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/5.GP_data/gp1_scripts_read2no00.txt.gz',sep='\t',dtype=str)
GP1_scr = pd.concat([GP1_scr_read2,GP1_scr_read2na])

GP1_drug_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/3.analgesic_code/d.GP1/GP1_drug_names_status.csv',sep='\t',dtype=str)
GP1_drug_code = GP1_drug_code[GP1_drug_code['status'] == 'y']
GP1_drug_code = GP1_drug_code.drop_duplicates(subset = 'drug_name')

GP1_scr_analgesic = GP1_scr[GP1_scr.drug_name.isin(GP1_drug_code.drug_name)]
GP1_scr_analgesic.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/6.GP_scripts_analgesic/GP1_scr_analgesic.txt.gz',sep='\t',index=False,header=True)

##B2: GP2 analgesic prescription records
GP2_scr_bnfna = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/5.GP_data/gp2_scripts_bnfna.txt.gz',sep='\t',dtype=str)
GP2_scr_bnf = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/5.GP_data/gp2_scripts_bnf.txt.gz',sep='\t',dtype=str)
GP2_scr = pd.concat([GP2_scr_bnf,GP2_scr_bnfna])

GP2_drug_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/3.analgesic_code/c.GP2/GP2_drug_names_status.csv',sep='\t',dtype=str)
GP2_drug_code = GP2_drug_code[GP2_drug_code['status'] == 'y']
GP2_drug_code = GP2_drug_code.drop_duplicates(subset = 'drug_name')

GP2_scr_analgesic = GP2_scr[GP2_scr.drug_name.isin(GP2_drug_code.drug_name)]
GP2_scr_analgesic.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/6.GP_scripts_analgesic/GP2_scr_analgesic.txt.gz',sep='\t',index=False,header=True)

##B3: GP3 analgesic prescription records
GP3_scr = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/3.analgesic_code/a.GP3/temporary_file/gp_reduced_3_analgesic.txt.gz', sep='\t',dtype=str)

GP3_drug_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/3.analgesic_code/a.GP3/GP3_drug_names_status.txt',sep='\t',dtype=str)
GP3_drug_code = GP3_drug_code[GP3_drug_code['status'] == 'y']
GP3_drug_code = GP3_drug_code.drop_duplicates(subset = 'drug_name')

GP3_scr_analgesic = GP3_scr[GP3_scr.drug_name.isin(GP3_drug_code.drug_name)]
GP3_scr_analgesic.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/6.GP_scripts_analgesic/GP3_scr_analgesic.txt.gz',sep='\t',index=False,header=True)

##B4: GP4 analgesic prescription records
GP4_scr = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/5.GP_data/gp4_scripts.txt.gz',sep='\t',dtype=str)

GP4_drug_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/3.analgesic_code/b.GP4/GP4_drug_names_status.txt',sep='\t',dtype=str)
GP4_drug_code = GP4_drug_code[GP4_drug_code['status'] == 'y']
GP4_drug_code = GP4_drug_code.drop_duplicates(subset = 'read_code')

GP4_scr_analgesic = GP4_scr[GP4_scr.read_2.isin(GP4_drug_code.read_code)]
GP4_scr_analgesic.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/6.GP_scripts_analgesic/GP4_scr_analgesic.txt.gz',sep='\t',index=False,header=True)


###Part C: QC

##import data
## import abdominal operation event date
hr_abd_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_dates.txt',sep='\t',dtype=str,header=0,names=['eid','event_dt'])
#gp_abd_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP/input/4.operation_demo_subset/gp_operation_dates.txt',sep='\t',dtype=str,header=0,names=['eid','event_dt'])

'''
###test if first HR is always ealier than GP
###conclusion: HR date is alway earlier or equal to GP, so subject's HR record should be prioritized. 
hr_abd_date_first = hr_abd_date
hr_abd_date_first['first'] = hr_abd_date_first.event_dt.str.split('-',1).str[0]
hr_abd_date_first = hr_abd_date_first.drop('event_dt',axis = 'columns')
gp_abd_date_first = gp_abd_date
gp_abd_date_first['first'] = gp_abd_date_first.event_dt.str.split('-',1).str[0]
gp_abd_date_first = gp_abd_date_first.drop('event_dt',axis = 'columns')
hrgp_abd = pd.merge(gp_abd_date_first,gp_abd_date_first,how='inner',on = 'eid')
hrgp_abd['earlier'] = pd.to_datetime(hrgp_abd.first_x) <= pd.to_datetime(hrgp_abd.first_y)
hrgp_abd.loc[hrgp_abd['earlier'] == 'False']
'''

gpponly_abd_eid = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/gpponly_operations_eid.txt',sep='\t',dtype=str,header=0,names=['eid','event_dt'])
hrgp_abd_eid = pd.concat([hr_abd_date,gpponly_abd_eid])
del [gpponly_abd_eid,hr_abd_date]


##C0: QC: remove patients with operation date before cut-off or invalid
qc_invalid_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc_invalid_date.txt.gz',sep='\t', dtype=str)
hrgp_abd_eid = hrgp_abd_eid[~hrgp_abd_eid.eid.isin(qc_invalid_date.eid)]

##C1: QC data: select patients with any prescription record, by intersecting abd and script data
gp_scripts_eid_withany = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/gp_scripts_eid_withany.txt.gz',sep='\t', dtype=str)
hrgp_abd_eid = hrgp_abd_eid[hrgp_abd_eid.eid.isin(gp_scripts_eid_withany.eid)]

del[gp_scripts_eid_withany,qc_invalid_date]

## C2: Remove patients with another abd operation during follow up
#create varibles for later
hrgp_abd_eid['event_dt_spl'] = hrgp_abd_eid.event_dt.str.split('/')
hrgp_abd_eid['unique_date'] = hrgp_abd_eid.event_dt_spl.apply(lambda x: list(set(x)))
hrgp_abd_eid['length'] = hrgp_abd_eid.unique_date.apply(lambda x: len(x))
hrgp_abd_eid['event_datetime'] =  hrgp_abd_eid.unique_date.apply(lambda x:  pd.to_datetime(x))
hrgp_abd_eid['min_datetime'] =  hrgp_abd_eid.event_datetime.apply(lambda x:  min(x))

#deal subjects with one date and subjects with two more dates separately
hrgp_abd_eid_oneabd = hrgp_abd_eid[hrgp_abd_eid['length'] == 1]
hrgp_abd_eid_twoabd = hrgp_abd_eid[hrgp_abd_eid['length'] > 1]
hrgp_abd_eid_twoabd['time_diff'] = hrgp_abd_eid_twoabd.event_datetime.apply(lambda x: x - min(x))
hrgp_abd_eid_twoabd['second_abd'] = hrgp_abd_eid_twoabd.time_diff.apply(lambda x: np.any(np.logical_and(x > datetime.timedelta(0) , x < datetime.timedelta(360))))

qc2_rm_eid = hrgp_abd_eid_twoabd.loc[hrgp_abd_eid_twoabd['second_abd'],'eid'] #keep eid with second_abd == True
hrgp_abd_eid_twoabd = hrgp_abd_eid_twoabd[~hrgp_abd_eid_twoabd.eid.isin(qc2_rm_eid)] #remove eid with second_abd == True
hrgp_abd_eid_twoabd = hrgp_abd_eid_twoabd.drop(['time_diff','second_abd'],axis = 'columns')

hrgp_abd_eid_qc2 = pd.concat([hrgp_abd_eid_oneabd,hrgp_abd_eid_twoabd])
qc2_rm_eid.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc2_rm_eid.txt.gz',sep='\t',index=False,header=True)

del [hrgp_abd_eid_oneabd,hrgp_abd_eid_twoabd,hrgp_abd_eid,qc2_rm_eid]

### C3: exclude patients with first operation after June 2015
#earliest prescriptions end in June 2016, which is gp3. as gp3 with most subjects, so just set a uniform cut-off for convience
subjectlist_qc3 = hrgp_abd_eid_qc2.loc[hrgp_abd_eid_qc2['min_datetime'] > pd.Timestamp(ts_input = '2015-06-30'),'eid']
hrgp_abd_eid_qc3 = hrgp_abd_eid_qc2[~hrgp_abd_eid_qc2.eid.isin(subjectlist_qc3)]
subjectlist_qc3.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc3_rm_eid.txt.gz',sep='\t',index=False,header=True)


'''
test = anal_scr_date.drop_duplicates(['eid','data_provider'])
test2 = test[test.eid.isin(patients_to_exclude)]
from collections import Counter
Counter(test2.data_provider)
'''
del [subjectlist_qc3,hrgp_abd_eid_qc2]

### C4: remove subjects who died during 1-year follow-up
##concatenate demographic date of HR and GP abd subjects
hr_de = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/raw_abd_operation/abd_operation_HR.txt.gz',sep='\t',dtype=str,usecols = ['n_eid','s_40000_0_0'])
gp_de = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/raw_abd_operation/GP_abd_demogra.txt.gz',sep='\t',dtype=str,usecols = ['n_eid','s_40000_0_0'])
de_list = pd.concat([hr_de,gp_de])
de_list = de_list [-de_list.s_40000_0_0.isna()]
de_list = de_list.rename(columns={"n_eid": "eid", "s_40000_0_0": "s_40000_0_0"})

'''
#As s_40000_1_0 include all info in s_40000_1_0, so just use s_40000_0_0 is enough. 
'''
##check deceased subjects only
hrgp_abd_eid_qc3_tocheck = hrgp_abd_eid_qc3[hrgp_abd_eid_qc3.eid.isin(de_list.eid)]
hrgp_abd_eid_qc3_tocheck = hrgp_abd_eid_qc3_tocheck.merge(de_list, how='left', on = 'eid')
hrgp_abd_eid_qc3_tocheck['s_40000'] = pd.to_timedelta(pd.to_numeric(hrgp_abd_eid_qc3_tocheck.s_40000_0_0), unit = 'D') + pd.Timestamp('1960-1-1')

hrgp_abd_eid_qc3_tocheck['death_in_followup']= np.where((hrgp_abd_eid_qc3_tocheck['s_40000'] >= hrgp_abd_eid_qc3_tocheck['min_datetime']) &
                                                        (hrgp_abd_eid_qc3_tocheck['s_40000'] <= (hrgp_abd_eid_qc3_tocheck['min_datetime'] + datetime.timedelta(360))),
                                                        'True', 'False')
subjectlist_qc4 = hrgp_abd_eid_qc3_tocheck[hrgp_abd_eid_qc3_tocheck['death_in_followup'] == 'True']
subjectlist_qc4 = subjectlist_qc4.drop_duplicates(subset = 'eid')
hrgp_abd_eid_qc4 = hrgp_abd_eid_qc3[-hrgp_abd_eid_qc3.eid.isin(subjectlist_qc4.eid)]
subjectlist_qc4.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/qc4_rm_eid.txt.gz',sep='\t',index=False,header=True)

del [de_list,gp_de,hr_de,hrgp_abd_eid_qc3_tocheck,hrgp_abd_eid_qc3,subjectlist_qc4]


### C5: keep subject who pass other sample QC
subjectlist_qc5 = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/qc_rm_eid/participants_passQC.csv',sep='\t', dtype = str)
hrgp_abd_eid_qc5 = hrgp_abd_eid_qc4[hrgp_abd_eid_qc4.eid.isin(subjectlist_qc5.n_eid)]

del [hrgp_abd_eid_qc4, subjectlist_qc5]

### C6: QC analgesic prescription record for later phenotype file generation

anal_scr_date = pd.DataFrame()
temp_list = []

for e in '1234':
    scr_date_temp = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/6.GP_scripts_analgesic/GP'+e+'_scr_analgesic.txt.gz',
                                sep='\t', dtype=str, usecols=['eid', 'drug_name', 'issue_date', 'read_2'])
    temp_list.append(scr_date_temp)

anal_scr_date = pd.concat(temp_list, ignore_index=True)


##QC: remove prescription record without date
anal_scr_date = anal_scr_date[anal_scr_date.issue_date.apply(lambda x: type(x)!=float)]
del scr_date_temp
##keep eid of scripts data in QCed list
anal_scr_date = anal_scr_date[anal_scr_date.eid.isin(hrgp_abd_eid_qc5.eid)]
anal_scr_date = anal_scr_date.drop_duplicates(['eid','issue_date','drug_name','read_2'])
anal_scr_date['dt'] = pd.to_datetime(anal_scr_date.issue_date, format='%d/%m/%Y', errors='coerce') #transform date as the same format as operation data
anal_scr_date['issue_date'] = anal_scr_date.dt.astype(str)
anal_scr_date = anal_scr_date.drop('dt',axis = 'columns')
anal_scr_date = anal_scr_date.sort_values(by = ['eid','issue_date'])




###Part D: create CPSP phenotype
##D1: mark first script record for each eid
anal_scr_date = anal_scr_date.set_index('eid',drop=False)
anal_scr_date['flag'] = np.where((anal_scr_date.eid != anal_scr_date.eid.shift()), 1, 0)

alldfs = [var for var in dir() if isinstance(eval(var), pd.core.frame.DataFrame)]
print(alldfs)

del [GP1_drug_code, GP1_scr, GP1_scr_analgesic, GP1_scr_read2, GP1_scr_read2na, GP2_drug_code, GP2_scr, GP2_scr_analgesic, GP2_scr_bnf, GP2_scr_bnfna, GP3_drug_code, GP3_scr, GP3_scr_analgesic, GP4_drug_code, GP4_scr, GP4_scr_analgesic, dup_gp_opdate_df, gp_abd_date, gp_operations, gp_operations_invalidlist, hr, hr_abd_date_temp, hr_abd_date_temp2, hr_abd_date_temp3, hr_abd_date_temp4, hr_abd_date_temp5, hr_abd_ope, hr_abd_ope_date, hr_operation_dates, hr_operations_invalidlist, opcs4_code, uni_gp_opdate]
del [alldfs,date_cutoff,date_opr_mask,dup_gp_opdate,e,mask_value,opr_mask,var_list]


#prepare uni and dup script file separately
dup_index_list = anal_scr_date.index[anal_scr_date.index.duplicated(keep=False)] # #mark all duplicated eid index as TRUE, and extract all duplicated eid index
dup_index_list = dup_index_list.drop_duplicates()
dup_scr_date = anal_scr_date.loc[dup_index_list].apply(lambda x: '/'.join(anal_scr_date.loc[x['eid'],'issue_date']), axis = 1)
# '/'.join(['2012-01-11']) is all right, but '/'.join('2012-01-11') will goes wrong
#x is slice of anal_scr_date, [x['eid'],'issue_date'] select all issue_date under this eid, only join if x['flag'] == 1
dup_scr_date = dup_scr_date[dup_scr_date != '']
dup_scr_date = dup_scr_date.to_frame(name = 'issue_date')
dup_scr_date['eid'] = dup_scr_date.index
dup_scr_date = dup_scr_date.drop_duplicates()


uni_index_list = anal_scr_date.index[~anal_scr_date.index.isin(dup_index_list)]
uni_scr_date =  anal_scr_date.loc[uni_index_list,['eid','issue_date']]
gp_scr_date = pd.concat([dup_scr_date,uni_scr_date]) # concatenate temp with chem by row

##D2: merge with operatation data, select scripts date in range, and create final dataset adding prescription date as day-distance from operation
gp_scr_date['issue_dt_spl'] = gp_scr_date.issue_date.str.split('/')
gp_scr_date['issue_datetime'] =  gp_scr_date.issue_dt_spl.apply(lambda x:  pd.to_datetime(x))
gp_scr_date = gp_scr_date.reset_index(drop=True)
hrgp_abd_scr = hrgp_abd_eid_qc5.merge(gp_scr_date, how='left', on = 'eid')

hrgp_abd_scr_nan = hrgp_abd_scr[hrgp_abd_scr.issue_date.isna()]
hrgp_abd_scr_yes = hrgp_abd_scr[-hrgp_abd_scr.eid.isin(hrgp_abd_scr_nan.eid)]
hrgp_abd_scr_yes['diff_issue_ope'] = hrgp_abd_scr_yes.apply(lambda x: x['issue_datetime'] - x['min_datetime'],axis = 1)
##np.where return index, then use index to obtain days
hrgp_abd_scr_yes['pres_inrange'] = hrgp_abd_scr_yes.diff_issue_ope.apply(lambda x: x[np.where(
    (x >=  datetime.timedelta(-360)) &
    (x <=  datetime.timedelta(360)))])

hrgp_abd_scr_yes['pres_inrange'] = hrgp_abd_scr_yes.pres_inrange.apply(lambda x: '/'.join(x[~x.isna()].astype(str))) #MERGING IN ONE COLUMN THE DATES #DataFrame.apply(,axis indicate application of function to each row.)
hrgp_abd_scr_yes = hrgp_abd_scr_yes.loc[:,['eid','min_datetime','pres_inrange']]
hrgp_abd_scr_nan = hrgp_abd_scr_nan.loc[:,['eid','min_datetime']]
hrgp_abd_scr_final = pd.concat([hrgp_abd_scr_yes,hrgp_abd_scr_nan]) # concatenate temp with chem by row
hrgp_abd_scr_final['pres_inrange'] = hrgp_abd_scr_final.pres_inrange.str.replace(" days", "")

hrgp_abd_scr_final.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/7.create_phenotype_file/hrgp_abd_scr.txt.gz',sep='\t',index=False,header=True)




##D3: create varibles to define CPSP pheontype
hrgp_abd_scr = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/7.create_phenotype_file/hrgp_abd_scr.txt.gz',sep='\t')
print(hrgp_abd_scr.shape)

##D31: for eid without pres record
hrgp_abd_scr['status'] = ''
cpsp_ctr_nopres = hrgp_abd_scr[hrgp_abd_scr.pres_inrange.isna()]
cpsp_ctr_nopres.status = '1'  ##assign status for phenotype definition;eid with no prescriptions is considered as control

cpsp_ctr_nopres_dim = pd.DataFrame(cpsp_ctr_nopres.shape)
cpsp_ctr_nopres_dim.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/output/demographic_distribution/cpsp_ctr_nopres.txt',index=False)



##D32: for eid with pres record
hrgp_abd_scr_pres = hrgp_abd_scr[~hrgp_abd_scr.eid.isin(cpsp_ctr_nopres.eid)]
del [cpsp_ctr_nopres_dim,hrgp_abd_scr]

#def function to sum pres numbers in each month
def pres_per_month(pres_nr):
    pres_dict = {}
    for i in range(-12, 12): #zero key, right edge is not included
        pres_dict [i + 0.5] = len([p for p in pres_nr if int(p) // 30 == i])
    return pres_dict

#count pain prescription for each month
hrgp_abd_scr_pres['pres_sum'] = hrgp_abd_scr_pres.pres_inrange.str.split('/').apply(pres_per_month)


#######
os.chdir('/home/songli/PhD_project/UKB_CPSP_abd/input/7.create_phenotype_file')

###prescription number distribution
hrgp_abd_scr_pres_demo = hrgp_abd_scr_pres
#[hrgp_abd_scr_pres.pres_sum.apply(lambda x: (len([e for e in x if e<0 if x[e]!=0])>= pre_m))]
pre_item = [-11.5,-10.5,-9.5,-8.5,-7.5,-6.5,-5.5,-4.5,-3.5,-2.5,-1.5,-0.5]
post_item = [0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5]

hrgp_abd_scr_pres_demo['pre_scr'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: {i: x[i] for i in pre_item})
hrgp_abd_scr_pres_demo['post_scr'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: {i: x[i] for i in post_item})

hrgp_abd_scr_pres_demo['pre_scr_sum'] = hrgp_abd_scr_pres_demo.pre_scr.apply(lambda x: sum(x.values()))
hrgp_abd_scr_pres_demo['post_scr_sum'] = hrgp_abd_scr_pres_demo.post_scr.apply(lambda x: sum(x.values()))
#rgp_abd_scr_pres_demo['scr_sum'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: sum(x.values()))

hrgp_abd_scr_pres_demo['pre_mon'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: (len([e for e in x if e<0 if x[e]!=0])))
hrgp_abd_scr_pres_demo['post_mon'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: (len([e for e in x if e>0 if x[e]!=0])))
#hrgp_abd_scr_pres_demo['scr_mon'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: (len([e for e in x if x[e]!=0])))

#hrgp_abd_scr_pres_demo['pre_value'] = hrgp_abd_scr_pres_demo.apply(lambda x: x['pre_scr_sum']*x['pre_mon']/12,axis = 1)
#hrgp_abd_scr_pres_demo['post_value'] = hrgp_abd_scr_pres_demo.apply(lambda x: x['post_scr_sum']*x['post_mon']/12,axis = 1)
#hrgp_abd_scr_pres_demo['scr_value'] = hrgp_abd_scr_pres_demo.apply(lambda x: x['scr_sum']*x['scr_mon']/24,axis = 1)


###D4: define CPSP phenotype
#find non-null months
hrgp_abd_scr_pres_demo['pre_mon_sep'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: [e for e in x if e<0 if x[e]!=0])
hrgp_abd_scr_pres_demo['post_mon_sep'] = hrgp_abd_scr_pres_demo.pres_sum.apply(lambda x: [e for e in x if e>0 if x[e]!=0])


##find continuous prescrition months ###obsolete
'''
from itertools import groupby
from operator import itemgetter

def find_conseq(indat):
    #list_scr = []
    list_scr_len = []
    temp_list = []
    for k, g in groupby(enumerate(indat), lambda ix : ix[0] - ix[1]):
        temp_list = list(map(itemgetter(1), g))
        #list_scr.append(temp_list)
        list_scr_len.append(len(temp_list))
    return list_scr_len

hrgp_abd_scr_pres_demo['pre_mon_conseq_max'] = hrgp_abd_scr_pres_demo.pre_mon_sep.apply(lambda x: 0 if len(x)==0 else max(find_conseq(x)))
hrgp_abd_scr_pres_demo['post_mon_conseq_max'] = hrgp_abd_scr_pres_demo.post_mon_sep.apply(lambda x: 0 if len(x)==0 else max(find_conseq(x)))
'''

###define case and control, pre_mon < 3 and post_mon covered 0< x <= 3. 3 < x <= 6. 6 < x;
rm_pre_pain = hrgp_abd_scr_pres_demo[hrgp_abd_scr_pres_demo['pre_mon'] > 3]
hrgp_abd_scr_pres_demo_clean = hrgp_abd_scr_pres_demo[~hrgp_abd_scr_pres_demo.eid.isin(rm_pre_pain.eid)]
hrgp_abd_scr_pres_demo_clean.status[hrgp_abd_scr_pres_demo_clean['post_mon'] > 6] = '3'
hrgp_abd_scr_pres_demo_clean.status[(hrgp_abd_scr_pres_demo_clean['post_mon'] > 3) & (hrgp_abd_scr_pres_demo_clean['post_mon'] <= 6)] = '2'
hrgp_abd_scr_pres_demo_clean.status[hrgp_abd_scr_pres_demo_clean['post_mon'] <= 3] = '1'

cpsp_pheno_final = pd.concat([hrgp_abd_scr_pres_demo_clean,cpsp_ctr_nopres])
cpsp_pheno_final.to_csv('./cpsp_phenotype.txt.gz',sep='\t',index=False)



###D5: define CPSP phenotype based on 3 consective months
def indices(l):
    res = []
    for i in range(0,len(l)-3):
        if  l[i+1] - l[i] == 1.0 and l[i+2] - l[i] == 2.0 and l[i+3] - l[i] == 3.0:
            res.append(l[i])
    return res

hrgp_abd_scr_pres_demo_clean_copy = hrgp_abd_scr_pres_demo_clean.copy()
hrgp_abd_scr_pres_demo_clean_copy['phenotype_consec_flag']  = hrgp_abd_scr_pres_demo_clean['post_mon_sep'].apply(lambda x: indices(x) if x else [])
cpsp_ctr_nopres['phenotype_consec_flag'] = np.empty((len(cpsp_ctr_nopres), 0)).tolist()
cpsp_pheno_final_consec = pd.concat([hrgp_abd_scr_pres_demo_clean_copy,cpsp_ctr_nopres])
cpsp_pheno_final_consec['phenotype_consec']  = cpsp_pheno_final_consec['phenotype_consec_flag'].apply(lambda x: "CPSP" if x else "Control")

freq = cpsp_pheno_final_consec['phenotype_consec'].value_counts()
print(freq)
