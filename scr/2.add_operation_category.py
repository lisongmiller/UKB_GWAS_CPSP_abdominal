##################################################
## This script is to describe demographic stats between groups
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
from collections import Counter
import statsmodels.api as sm



###Part A: add category to codes map and operation data
os.chdir('/home/songli/PhD_project/UKB_CPSP_abd/output/demographic_distribution')

##A1: map Read code to OPCS4
#code list
opcs4_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/opcs4.txt',sep='\t',names=['codes'],dtype=str)
read2_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/readv2.txt',sep='\t',names=['readv2'],header = 0,dtype=str)
read3_code = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/readv3.txt',sep='\t',names=['readv3'],header = 0,dtype=str)

#map readv2 to opcs4 code
readv2_opcs4 = pd.read_excel('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/temp_file/primarycare_codings/all_lkps_maps_v3.xlsx',sheet_name = 'read_v2_opcs4', dtype = str)
readv2_opcs4 = readv2_opcs4.rename(columns={"read_code": "readv2",'opcs_4.2_code': 'opcs4'})
read2_code_clean = pd.merge(read2_code,readv2_opcs4,how='left',on = 'readv2')
read2_code_clean['chapter'] = read2_code_clean.opcs4.str.slice(0,1)
read2_code_clean['category'] = ''
read2_code_clean.category[read2_code_clean.chapter.isin(['G','H','J']) ] = 'Digestive'
read2_code_clean.category[read2_code_clean.chapter.isin(['L']) ] = 'Arteries_Veins'
read2_code_clean.category[read2_code_clean.chapter.isin(['M']) ] = 'Urinary'
read2_code_clean.category[read2_code_clean.chapter.isin(['P','Q','R']) ] = 'F_Genital_Tract'
read2_code_clean.category[read2_code_clean.readv2.str.startswith("L398")] = 'F_Genital_Tract';
read2_code_clean.category[read2_code_clean.chapter.isin(['T']) ] = 'Soft_tissue'
read2_code_clean.category[read2_code_clean.chapter.isin(['Y']) ] = 'Laparoscopy'
read2_code_clean.category[read2_code_clean.chapter.isin(['Y']) ] = 'Laparoscopy'

read2_code_clean = read2_code_clean.drop_duplicates(subset = ["readv2","category"])
read2_code_clean.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/read2_chapter.txt',sep = '\t',index=False)

#map readv3 to opcs4 code
readv3_opcs4 = pd.read_excel('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/temp_file/primarycare_codings/all_lkps_maps_v3.xlsx',sheet_name = 'read_ctv3_opcs4', dtype = str)
readv3_opcs4 = readv3_opcs4.rename(columns={"read_code": "readv3", 'opcs4_code': 'opcs4'})
readv3_opcs4 = readv3_opcs4[readv3_opcs4.mapping_status != 'A']
read3_code_temp = pd.merge(read3_code,readv3_opcs4,how='left',on = 'readv3')
read3_code_temp = read3_code_temp.drop_duplicates(subset = ["readv3"]) #normally the first map is good enough
read3_code_temp = read3_code_temp.loc[:,['readv3','opcs4','mapping_status']]

#some readv3 codes without OPCS4 mapping codes, but they are the same with readv2 codes. So readv2 codes are used for mapping
read3_code_temp_na = read3_code_temp[read3_code_temp.opcs4.isna()].loc[:,["readv3","mapping_status"]]
read3_code_temp_na = read3_code_temp_na.rename(columns={'readv3': 'readv2'})
read3_code_temp_na = pd.merge(read3_code_temp_na,read2_code_clean,how='left',on = 'readv2')
read3_code_temp_na = read3_code_temp_na.rename(columns={'readv2': 'readv3'})
read3_code_temp_na = read3_code_temp_na.loc[:,['readv3','opcs4','category']]

#assign category to original file, and concate NA file with non-NA file
read3_code_temp = read3_code_temp[~read3_code_temp.opcs4.isna()]
read3_code_temp['chapter'] = read3_code_temp.opcs4.str.slice(0,1)
read3_code_temp['category'] = ''
read3_code_temp.category[read3_code_temp.chapter.isin(['G','H','J']) ] = 'Digestive'
read3_code_temp.category[read3_code_temp.chapter.isin(['L']) ] = 'Arteries_Veins'
read3_code_temp.category[read3_code_temp.chapter.isin(['M']) ] = 'Urinary'
read3_code_temp.category[read3_code_temp.chapter.isin(['P','Q','R']) ] = 'F_Genital_Tract'
read3_code_temp.category[read3_code_temp.chapter.isin(['T']) ] = 'Soft_tissue'
read3_code_temp.category[read3_code_temp.chapter.isin(['Y']) ] = 'Laparoscopy'
read3_code_temp.category[read3_code_temp.readv3 == "X20VC" ] = 'Digestive' #This code needs to be mapped manually

read3_code_clean = pd.concat([read3_code_temp,read3_code_temp_na])
read3_code_clean.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/read3_chapter.txt',sep = '\t',index=False)




###A2: generate cpsp phenotype by chapter file
cpsp_phenotype = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/7.create_phenotype_file/cpsp_phenotype.txt.gz',sep='\t',dtype = str)
cpsp_phenotype = cpsp_phenotype.rename(columns={"min_datetime": "s_41282"})

#merge CPSP phenotype file wtih operation code and date file
hr_abd_ope_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/hr_operation_codes_dates.txt',sep='\t',dtype = str)
cpsp_code_date = pd.merge(cpsp_phenotype,hr_abd_ope_date,how='left',on = ['eid','s_41282'])
cpsp_code_date = cpsp_code_date.loc[:,['eid','s_41282','s_41272','status']]

#generate eid first abd operation code with date by HR and GP separately
hr_abd_ope_first = cpsp_code_date[~cpsp_code_date.s_41272.isna()]
gp_abd_ope_first = cpsp_code_date[cpsp_code_date.s_41272.isna()]

##A21: check HR subjects operation numbers and chapters
freq_hr = pd.DataFrame(hr_abd_ope_first['eid'].value_counts())
freq_hr = freq_hr.rename(columns={'eid': 'freq'})
freq_hr['eid'] = freq_hr.index
hr_abd_ope_first = pd.merge(hr_abd_ope_first,freq_hr,how='left',on = ['eid'])
hr_abd_ope_first['chapter'] = hr_abd_ope_first.s_41272.str.slice(0,1)

hr_abd_ope_first['category'] = ''
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['G','H','J']) ] = 'Digestive'
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['L']) ] = 'Arteries_Veins'
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['M']) ] = 'Urinary'
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['P','Q','R']) ] = 'F_Genital_Tract'
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['T']) ] = 'Soft_tissue'
hr_abd_ope_first.category[hr_abd_ope_first.chapter.isin(['Y']) ] = 'Laparoscopy'

##A22: check GP subjects operation numbers and chapters
gp_abd_ope_date = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/4.operation_demo_subset/raw_abd_operation/abd_operation_GP.txt.gz',sep='\t',dtype = str,usecols = ['eid','event_dt','read_2','read_3'])
gp_abd_ope_date['s_41282'] = pd.to_datetime(gp_abd_ope_date['event_dt']).astype(str)
gp_abd_ope_date = gp_abd_ope_date.drop(['event_dt'],axis = 1)
gp_abd_ope_date = gp_abd_ope_date.drop_duplicates(subset = ['eid','read_2','read_3','s_41282',])
gp_abd_ope_first = pd.merge(gp_abd_ope_first,gp_abd_ope_date,how='left',on = ['eid','s_41282'])
gp_abd_ope_first_r2 = gp_abd_ope_first[~gp_abd_ope_first.read_2.isna()]
gp_abd_ope_first_r3 = gp_abd_ope_first[gp_abd_ope_first.read_2.isna()]

##check readv2
read2_code_clean = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/read2_chapter.txt',sep = '\t')
read2_code_clean = read2_code_clean.rename(columns={'readv2': 'read_2'})

gp_abd_ope_first_r2 = pd.merge(gp_abd_ope_first_r2,read2_code_clean,how='left',on = ['read_2'])
gp_abd_ope_first_r2 = gp_abd_ope_first_r2.loc[:,['eid','s_41282','status','category']]

##check readv3
read3_code_clean = pd.read_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/1.operation_code/read3_chapter.txt',sep = '\t')
read3_code_clean = read3_code_clean.rename(columns={'readv3': 'read_3','opcs4_code' : 'opcs4'})
read3_code_clean = read3_code_clean.loc[:,['read_3','category']]

gp_abd_ope_first_r3 = pd.merge(gp_abd_ope_first_r3,read3_code_clean,how='left',on = ['read_3'])
gp_abd_ope_first_r3 = gp_abd_ope_first_r3.loc[:,['eid','s_41282','status','category']]
gp_abd_ope_first_clean = pd.concat([gp_abd_ope_first_r2,gp_abd_ope_first_r3])

freq_gp = pd.DataFrame(gp_abd_ope_first_clean['eid'].value_counts())
freq_gp = freq_gp.rename(columns={'eid': 'freq'})
freq_gp['eid'] = freq_gp.index
gp_abd_ope_first_clean = pd.merge(gp_abd_ope_first_clean,freq_gp,how='left',on = ['eid'])

del [cpsp_phenotype,cpsp_code_date,freq_gp,freq_hr,gp_abd_ope_date,gp_abd_ope_first,gp_abd_ope_first_r2,gp_abd_ope_first_r3,
     hr_abd_ope_date,read2_code_clean,read3_code_clean]

###A23 merge HR and GP data
hr_abd_ope_first = hr_abd_ope_first.drop('s_41272',axis = 1)
hr_abd_ope_first['dt_source'] = 'HR'
gp_abd_ope_first_clean['dt_source'] = 'GP'
abd_ope_first = pd.concat([hr_abd_ope_first,gp_abd_ope_first_clean])
abd_ope_first.to_csv('/home/songli/PhD_project/UKB_CPSP_abd/input/7.create_phenotype_file/cpsp_phenotype_chapter.txt.gz',header = True, index = False, sep = '\t')

