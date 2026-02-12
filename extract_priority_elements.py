# Code to call Essence API and download data for Data Quality Dashboard
# Author: Sandeep Chintabathina
# January 2026
# 
#  

from pynssp import *
from datetime import timedelta,date
import pandas as pd
import numpy as np
from essence_credentials import get_profile
import requests

#race mapping
map_dict ={'1002-5':'American Indian or Alaska Native',
           '2028-9':'Asian',
           '2029-7':'Asian', # Asian Indian
           '2054-5':'Black or African American',
           '2076-8':'Native Hawaiian or Pacific Islander',
           '2079-2':'Native Hawaiian',
           '2106-3':'White',
           '2113-9':'White', # Irish
           '2114-7':'White', # Italian
           '2131-1':'Other',
           '2034-7':'Chinese',
           '2036-2':'Filipino',
           '2039-6':'Japanese',
           '2040-4':'Korean',
           '2046-1':'Asian', # Thai
           '2047-9':'Asian', # Vietnamese
           '2080-0':'Samoan',
           '2082-6':'Tongan', 
           '2085-9':'Other Micronesian',
           '2090-9':'Marshallese', 
           '2135-2':'Hispanic or Latino', # Other Hispanic or Latino
           '2148-5':'Hispanic or Latino', # Mexican
           '2180-8':'Hispanic or Latino', # Puerto Rican
           '2500-7':'Pacific Islander', # Other Pacific Islander
           '2081-8':'Pacific Islander', # Tahitian
           '2086-7':'Pacific Islander', # Guamanian or Chamorro
           '2129-5':'Asian', # Arab
           '2041-2':'Asian', # Laotian
           'NR':'Not Reported',
           'UNK':'Unknown'
           }

# Map race codes to race names
def map_to_races(somecodes):
    somecodes=somecodes.strip(';')
    tokens = somecodes.split(';')
    if len(tokens)==1:
        return map_dict[tokens[0]]
    else:
        #for token in tokens:
        #    if token=='2076-8' or token=='2079-2':
        #        return 'NHPI Multiracial'
            #elif token=='2079-2':
            #    return 'Native Hawaiian Multiracial'
        return 'Multiracial'

def analyze_race(race_sub: pd.DataFrame):
    
    # Map the race code column to names
    race_sub.loc[:,'Race_flat'] = [map_to_races(code) for code in race_sub.loc[:,'Race_flat']]

    # Next, group by races and find counts
    race_grouped = race_sub.groupby(['HospitalName','Race_flat']).size().reset_index(name='race_count')
    print(race_grouped)
    # Pivot to get the races in the columns
    race_pivoted = race_grouped.pivot(index='HospitalName',columns='Race_flat',values='race_count')
    # Fill Nan values
    race_pivoted=race_pivoted.fillna(0)
    # Make columns as int
    #for col in race_pivoted.columns:
    #    race_pivoted[col] = race_pivoted[col].astype(int)
 
    #Calculate num_visit
    for idx in race_pivoted.index:
        race_pivoted.loc[idx,'num_visits'] = race_pivoted.loc[idx,:].sum()

    # Calculate race percentages = race_counts/num_visits
    for col in race_pivoted.columns:
        for idx in race_pivoted.index:
            if col!='num_visits':
                race_pivoted.loc[idx,col] = round(race_pivoted.loc[idx,col]*100.0/race_pivoted.loc[idx,'num_visits'],2)

    total_visits = sum(race_pivoted.loc[:,'num_visits'])
    race_pivoted.loc[:,'percent_of_total_visits'] = [round(v*100/total_visits,2) for v in race_pivoted.loc[:,'num_visits']]

    race_pivoted.to_csv('output/race_breakdown.csv')

# A function to remove duplicates on a case by case basis 
def remove_duplicates(df_dupes):
    
    # Convert C_Visit_Date_Time to a date object
    df_dupes.loc[:,'C_Visit_Date_Time'] = [datetime.strptime(d,'%Y-%m-%d %H:%M:%S.%f') for d in df_dupes.loc[:,'C_Visit_Date_Time']]

    #For Hilo - keep the latest visit date
    df_hilo = df_dupes[df_dupes['HospitalName']=='HI-Hilo Medical Center']
    # Use transform() to get a rank for each row of the group based on visit_date_time 
    # transform() takes a series and returns a series as opposed to apply() which takes a dataframe returns a dataframe and may not have a rank for each row of the group
    # Prev method for verification
    #df_hilo.loc[:,'order'] = df_hilo.groupby(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'])['C_Visit_Date_Time'].transform(lambda x: x.rank())
    #df_hilo = df_hilo[df_hilo['order']>=2]

    # Pick the rows that match with the max value for visit date time
    max_idx =  df_hilo.groupby(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'])['C_Visit_Date_Time'].transform(max)==df_hilo['C_Visit_Date_Time']
    #print('max_index',max_idx)

    #df_hilo[max_idx].to_csv('output/hilo_order.csv',index=False)
    df_hilo = df_hilo[max_idx]
    print('df_hilo size:',len(df_hilo))

    #For Kau and Honokaa - keep the latest visit date
    df_kau = df_dupes[df_dupes['HospitalName']=="HI-Ka'u Hospital"]
    # Another way instead of max is to sort by visit times and keep the last duplicate since that is the latest time
    # Drawback: Will not return all rows in case of a tie
    df_kau = df_kau.sort_values('C_Visit_Date_Time').drop_duplicates(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'],keep='last')
    print('df_kau size: ',len(df_kau))
    
    #For Honokaa - keep the latest visit date
    df_honokaa = df_dupes[df_dupes['HospitalName']=="HI-Honoka'a Hospital"]
    df_honokaa = df_honokaa.sort_values('C_Visit_Date_Time').drop_duplicates(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'],keep='last')
    print('df_honokaa size',len(df_honokaa))

    # Kuakini - keep the latest visit date
    df_kuakini = df_dupes[df_dupes['HospitalName']=='HI-Kuakini Medical Center']
    df_kuakini = df_kuakini.sort_values('C_Visit_Date_Time').drop_duplicates(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'],keep='last')
    print('df_kuakini size',len(df_kuakini))
  
    # For Straub and HPH Urgent cares, all have same visit date times, so drop the empty diagnosis
    df_hph = df_dupes[df_dupes['HospitalName'].isin (['HI-Straub Clinic and Hospital','HI-Urgent Care - Straub Clinic Sheraton',
                                                   'HI-Urgent Care - Kahala Clinic and Urgent Care','HI-Urgent Care - Ward Village Clinic and Urgent Care'])]
    df_hph = df_hph[df_hph['DischargeDiagnosis']!='']
    print('df_hph size',len(df_hph))
    
    # QMC punchbowl,West Oahu, and Wahiawa - all of them are transfers from Wahiawa to QMC/WO and WO to QMC. We have ED to ED as well as Inpatient to Inpatient transfers
    # So keeping all of them
    df_qmc_wo_wahiawa = df_dupes[df_dupes['HospitalName'].isin(["HI-The Queen's Medical Center",'HI-Queens Medical Center West Oahu','HI-Wahiawa General Hospital'])]
    print('df_qmc_wo_wahiawa size',len(df_qmc_wo_wahiawa))
    
    # Combine all of them
    df_all = pd.concat([df_hilo,df_kau,df_honokaa,df_kuakini,df_hph,df_qmc_wo_wahiawa])
    print('df_all size',len(df_all))

    df_all.to_csv('output/non_duplicates.csv',index=False)
    return df_all




def main():
    # Determine a time period
    # Pick today as end date
    end_date = date.today()
    # Pick 30 days from end day for start date
    start_date = end_date- timedelta(days=30)
    print('Start Date: ',start_date,'End Date: ',end_date)

    # Uses stored username, password in get_profile function - This is for Essence
    myProfile = get_profile()

    # URL retrieved from "all_visits_no_filter_denominator" widget from "Data Quality" tab in Essence
    # Retrieves all priority 1,2, and 3 elements
    url="https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails/csv?datasource=va_er&medicalGroupingSystem=essencesyndromes&userId=4118&percentParam=noPercent&site=886&aqtTarget=DataDetails&geographySystem=state&detector=nodetectordetector&timeResolution=monthly&order1=HospitalName&order2=C_Processed_Facility_ID&order3=C_Visit_Date_Time&startDate=13Jan26&endDate=20Jan26"

    api_data = pd.DataFrame()
    try:
        api_data = pd.read_csv("raw_data/raw_data.csv",na_filter=False,dtype={'Age':object,'C_Patient_Age':object,'C_Unique_Patient_ID':object,'DischargeDisposition':object,'Sending_Facility_ID':object,'Treating_Facility_ID':object,'Smoking_Status_Code':object,'Visit_ID':object})
    except:
        # Change the dates to desires start and end dates
        url = change_dates(url, start_date = start_date, end_date = end_date)
        try:
            api_data = get_api_data(url, profile=myProfile,fromCSV=True,na_filter=False,encoding='latin-1')
            api_data.to_csv('raw_data/raw_data.csv',index=False)
        except requests.exceptions.ConnectionError as e:
            r = "No response"


    print('Number of rows',len(api_data))

    #print(api_data.loc[:,'C_Visit_Date_Time'].value_counts(dropna=False))
    #print(api_data.info())

    # Check overall duplicates
    print('# of Overall Duplicates',len(api_data[api_data.duplicated()]))

    # Determine if there are duplicate rows based on visit id, patient id, facility and patient class
    df_duplicates = api_data[api_data.duplicated(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'],keep=False)]

    print('# of duplicate patient id, visit id, patient class pairs',len(df_duplicates))

    df_duplicates[['HospitalName','C_Unique_Patient_ID','C_Visit_Date_Time',"Visit_ID",'C_Patient_Class','DischargeDiagnosis']].to_csv('output/duplicates.csv',index=False)

    print('List of facilities with duplicates\n',df_duplicates.loc[:,'HospitalName'].value_counts(dropna=False))

    # Drop all pairs of duplicates
    api_data = api_data.drop_duplicates(['C_Unique_Patient_ID','Visit_ID','C_Patient_Class'],keep=False)
    print('Number of rows after dropping duplicate pairs',len(api_data))
    # Obtain the set that is duplicate free
    dup_free=remove_duplicates(df_duplicates)
    api_data = pd.concat([api_data,dup_free])
    api_data=api_data.reset_index(drop=True)
    print(api_data)
    print('Number of rows after adding back one from each pair',len(api_data))

    # Start grouping by hospital name and data element to identify completeness

    # Process race data
    race_sub = api_data[['HospitalName','Race_flat']]

    #print(race_grouped)
    analyze_race(race_sub)

    api_data_new = api_data.copy()
    # Create a count column to act as denominator
    api_data_new['count']=1

    # Values that are placeholders for non reported values
    non_values =['none','','[]','Not Reported or Null','Not Categorized',';NR;','-1']

    # Convert each column to a binary valued column to help with computing completion rates
    value_columns = []
    for column in api_data_new.columns:
        if column!='HospitalName':
            api_data_new.loc[:,column] = [1 if v not in non_values else 0 for v in api_data_new.loc[:,column]]
            value_columns.append(column)

    # Pivot on hospital name and aggregate on remaining columns
    # Could also do groupby hospitalname and data element (get group count) and then pivot
    api_data_pivoted = api_data_new.pivot_table(index=['HospitalName'],values=value_columns,aggfunc='sum')

    # Calculate percentages - replace counts with percentages
    for col in api_data_pivoted.columns:
        for idx in api_data_pivoted.index:
            api_data_pivoted.loc[idx,col] = round(api_data_pivoted.loc[idx,col]*100/api_data_pivoted.loc[idx,'count'],2)

    print(api_data_pivoted)

    api_data_pivoted = api_data_pivoted.reset_index()
    priority1 = api_data_pivoted[['HospitalName','Admit_Date_Time', 'ChiefComplaintOrig',
       'DischargeDiagnosis', 'Age', 'C_Patient_Class', 'Facility_Type_Code',
       'C_Patient_County', 'C_FacType_Patient_Class', 
       'Visit_ID', 'C_Unique_Patient_ID', 'Patient_Zip', 'C_Death',
       'Treating_Facility_ID', 'Sending_Facility_ID', 'Trigger_Event',]]

    priority2 = api_data_pivoted[['HospitalName','DischargeDisposition', 'Race_flat',
       'c_race', 'Ethnicity_flat', 'c_ethnicity', 'Age', 'C_Patient_Age',
       'C_Patient_Age_Units', 'Birth_Date_Time', 'MedRecNo',
       'C_Patient_County', 'Patient_City', 'Patient_State', 'Patient_Country',
       'Discharge_Date_Time', 'Recorded_Date_Time', 'Diagnosis_Type',
       'Admit_Reason_Code']]
    
    priority3 = api_data_pivoted[['HospitalName', 'Height',
       'Height_Units', 'Weight', 'Weight_Units', 'Body_Mass_Index',
       'Smoking_Status_Code', 'DeathIndicator', 'Death_Date_Time',
       'Pregnancy_Status_Code', 'Travel_History', 'Initial_Acuity_Code',
       'Initial_Acuity_Combo', 'TriageNotesOrig', 'Systolic_Blood_Pressure',
       'Systolic_Blood_Pressure_Units', 'Diastolic_Blood_Pressure',
       'Diastolic_Blood_Pressure_Units', 'Systolic_Diastolic_Blood_Pressure',
       'Systolic_Diastolic_Blood_Pressure_Units', 'Initial_Pulse_Oximetry',
       'Initial_Temp', 'Admit_Source', 'Admission_Type', 'Onset_Date',
       'ClinicalImpression', 'Hospital_Unit_Code', 'Hospital_Unit_Description',
       'Problem_List_Code', 'Problem_List_Combo',
       'Medication_List', 'Medication_Code', 'Medication_Combo',
       'Procedure_Code', 'Procedure_Combo', 'Insurance_Coverage',
       'Insurance_Company_ID', 
       'Procedure_Date_Time', 'Diagnosis_Date_Time']]
    
    priority1.to_csv('output/priority_1.csv',index=False)
    priority2.to_csv('output/priority_2.csv',index=False)
    priority3.to_csv('output/priority_3.csv',index=False)
    
    print('Done')





# Using this prevents this code from being called by an import statement
# Allows us to control where the program starts executing code 
# Code only runs if run directly by python interpreter
if __name__ =='__main__':
    main()
