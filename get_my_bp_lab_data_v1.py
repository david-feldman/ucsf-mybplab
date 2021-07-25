#!/usr/bin/env python
#required pacakges
import pandas as pd
import numpy as np
import synapseclient
import sys
import getpass
import multiprocessing
import os
from synapseclient import build_table
import json
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import pytz
#import psutil


def get_synapse_credentials():
    print('Please input your Synapse username and password.')
    print('Username: ', end='',flush=True)
    user = sys.stdin.readline().rstrip()
    p = getpass.getpass(prompt='Password: ')
    return user, p

def login_to_synapse(creds):
    try:
        syn = synapseclient.Synapse()
        my_username = creds[0]
        my_password = creds[1]
        syn.login(my_username, my_password)
        return syn
    except:
        print('Invalid credentials. Please try again.')
        sys.exit()
def get_data_from_synapse_table(syn_connection, curr_table):
    print('Retrieving data from {} ({})'.format(curr_table['table_name'], curr_table['table_label']))
    table = syn_connection.tableQuery('select * from ' + curr_table['table_name'])
    df = table.asDataFrame()
    return  df, table

def get_data_from_many_tables(syn_connection,table_dicts, id_filter = []):
    dfs = []
    for curr in table_dicts:
        df, table = get_data_from_synapse_table(syn_connection,curr)
        #print(curr["table_label"], "pre-filter", df.size)
        # we dont want to remove v1 data from enhanced profile data but we do want to remove from everything else
        if len(id_filter) > 0 and "Enhance Profile" not in curr['table_label']:
            df = df[df["recordId"].isin(id_filter)]
        dfs.append({"table_label":curr['table_label'],"dataframe":df,"synapse_table":table})
        #print(curr["table_label"], "post-filter", df.size)
    return dfs

def get_table_mapping_from_local_file(fl):
    tables = pd.read_csv(fl)
    outdicts = []
    for idx, row in tables.iterrows():
        outdicts.append({'table_name' : row['table_name'], 'table_label' : row['table_label']})
    return outdicts

def get_relevant_tables_and_record_list(syn_connection, table_dicts):
      ht = [d for d in table_dicts  if d['table_label'] == 'Health Data Summary Table']
      health_data, table = get_data_from_synapse_table(syn_connection,ht[0])
      #based upon offline list delivered by Sage
      mybp_lab1_versions = ['version 1.0.3, build 72',  'version 1.0.5, build 75', 'version 1.0.6, build 76',  'version 1.1.0, build 82',  'version 1.1.4, build 86', 'version 1.1.5, build 88']
      health_data_filtered = health_data[health_data["appVersion"].isin(mybp_lab1_versions)]
      relevant_table_list = health_data_filtered['originalTable'].unique().tolist()
      #tables excluded based upon offline requirements
      exclude_tables = []
      relevant_table_list = [tab for tab in relevant_table_list if tab not in exclude_tables]
      print(relevant_table_list)
      record_list = health_data_filtered['recordId'].unique().tolist()
      relevant_table_dicts = [d for d in table_dicts if d['table_label'] in relevant_table_list]
      return relevant_table_dicts, record_list

def create_tables_and_columns_csv(dataframe_dicts):
    outdicts = []
    for d in dataframe_dicts:
        for c in d['dataframe'].columns:
            outdicts.append({'table':d["table_label"],"column":c})
    pd.DataFrame(outdicts).to_csv('v2_table_and_column_list.csv',index=False)

def generate_list_of_task_types(syn_connection, dataframe_dicts):
    list_of_unique_Cog_Task_Type = []
    list_of_unique_Intervention_Task_Type = []
    list_of_unique_Cog_Task_Test_Name = []
    list_of_unique_Intervention_Task_Group = []
    cog_dat = pd.DataFrame()
    for d in dataframe_dicts:
         for c in d['dataframe'].columns:
             if c == 'answers.Cog_Task_Test_Name':
                 #cog_dat = cog_dat.append(d['dataframe'][['answers.Cog_Task_Test_Name','Cog_Result.json']])
                 #print(d["table_label"],c)
                 list_of_unique_Cog_Task_Test_Name.extend(d["dataframe"][c].unique().tolist())

             if c == 'answers.Cog_Task_Type':
                 #print(d["table_label"],c)
                 list_of_unique_Cog_Task_Type.extend(d["dataframe"][c].unique().tolist())

             if c == 'answers.Intervention_Task_Group':
                 list_of_unique_Intervention_Task_Group.extend(d["dataframe"][c].unique().tolist())
                 print(d["table_label"],c)

             if c == 'answers.Intervention_Task_Type':
                  list_of_unique_Intervention_Task_Type.extend(d["dataframe"][c].unique().tolist())
                  print(d["table_label"],c)

    list_of_unique_Cog_Task_Type = [x for x in list_of_unique_Cog_Task_Type if str(x) != 'nan']
    list_of_unique_Cog_Task_Test_Name = [x for x in list_of_unique_Cog_Task_Test_Name if str(x) != 'nan']
    list_of_unique_Intervention_Task_Type = [x for x in list_of_unique_Intervention_Task_Type if str(x) != 'nan']
    list_of_unique_Intervention_Task_Group = [x for x in list_of_unique_Intervention_Task_Group if str(x) != 'nan']
    print("Cog Task Test Names: ", sorted(set(list_of_unique_Cog_Task_Test_Name)))
    print("Cog Task Test Types: ",sorted(set(list_of_unique_Cog_Task_Type)))
    print("Intervention  Task Test Types: ",sorted(set(list_of_unique_Intervention_Task_Type)))
    print("Intervention Task Test Groups: ",sorted(set(list_of_unique_Intervention_Task_Group)))

    #cog_dat = cog_dat.dropna()
    #table = build_table("simple_table", "syn123", cog_dat.head())
    #file_map = syn_connection.downloadTableColumns(table, ['Cog_Result.json'])
    #print(cog_dat.head())


def download_jsons_and_assemble_metadata(syn_connection, dataframe_dicts):
    file_handle_dicts = []
    bodymap_table_list = ['Night-v8','Night-v9']
    bp_metadata_df = pd.DataFrame()
    bmap_back_metadata_df = pd.DataFrame()
    bmap_front_metadata_df = pd.DataFrame()

    for d in dataframe_dicts:
        if set(['blood_pressure_stress_recorder_bloodPressure.json']).issubset(d["dataframe"].columns):
            print("json column found!") 
            file_map = syn_connection.downloadTableColumns(d["synapse_table"], ['blood_pressure_stress_recorder_bloodPressure.json'])
            print("completed file map!")
            for file_handle_id, path in file_map.items():
                 file_handle_dicts.append({"table_label":d["table_label"],"file_handle_id":int(file_handle_id),"path":path,"type":"BP"})
            temp_df = d['dataframe'][['healthCode','recordId','blood_pressure_stress_recorder_bloodPressure.json']].copy(deep=True).dropna(subset=['healthCode', 'blood_pressure_stress_recorder_bloodPressure.json'])
            temp_df['table_label'] = d['table_label']
            temp_df = temp_df.rename({"blood_pressure_stress_recorder_bloodPressure.json":"file_handle_id"},errors="raise",axis=1)
            temp_df = temp_df.astype({"file_handle_id": int,"table_label": str})
            bp_metadata_df = bp_metadata_df.append(temp_df)

        if d["table_label"] in bodymap_table_list:
             file_map = syn_connection.downloadTableColumns(d["synapse_table"], ['bodyMapBack_bodyMapBack.json'])
             for file_handle_id, path in file_map.items():
                 file_handle_dicts.append({"table_label":d["table_label"],"file_handle_id":int(file_handle_id),"path":path,"type":"BodyMapBack"})
             temp_df = d['dataframe'][['healthCode','recordId','bodyMapBack_bodyMapBack.json']].copy(deep=True).dropna()
             temp_df['table_label'] = d['table_label']
             temp_df = temp_df.rename({"bodyMapBack_bodyMapBack.json":"file_handle_id"},errors="raise",axis=1)
             temp_df = temp_df.astype({"file_handle_id":int,"table_label": str})
             bmap_back_metadata_df = bmap_back_metadata_df.append(temp_df)

             file_map = syn_connection.downloadTableColumns(d["synapse_table"], ['bodyMapFront_bodyMapFront.json'])
             for file_handle_id, path in file_map.items():
                 file_handle_dicts.append({"table_label":d["table_label"],"file_handle_id":int(file_handle_id),"path":path,"type":"BodyMapFront"})
             temp_df = d['dataframe'][['healthCode','recordId','bodyMapFront_bodyMapFront.json']].copy(deep=True).dropna()
             temp_df['table_label'] = d['table_label']
             temp_df = temp_df.rename({"bodyMapFront_bodyMapFront.json":"file_handle_id"},errors="raise",axis=1)
             temp_df = temp_df.astype({"file_handle_id":int,"table_label": str})
             bmap_front_metadata_df = bmap_front_metadata_df.append(temp_df)

    file_handle_and_path_df = pd.DataFrame(file_handle_dicts)
    bp_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'BP'].reset_index(drop=True).merge(bp_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    bmap_back_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'BodyMapBack'].reset_index(drop=True).merge(bmap_back_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    bmap_front_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'BodyMapFront'].reset_index(drop=True).merge(bmap_front_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    return bp_output_data, bmap_back_json_output_data, bmap_front_json_output_data

def extract_bodymap_data(bmap_front_df, bmap_back_df):
    # image sample dimensions
    MALE_FRONT_RATIO = 692/ 1672
    FEMALE_FRONT_RATIO = 576/ 1672
    MALE_BACK_RATIO = 716/ 1672
    FEMALE_BACK_RATIO = 612/ 1672

    output_full_dicts = []
    output_summary_dicts = []

    bmap_front_df.to_csv("bmap_front.csv")
    bmap_back_df.to_csv("bmap_back.csv")

    for x in [{'df':bmap_back_df,'male':MALE_BACK_RATIO,'female':FEMALE_BACK_RATIO},{'df':bmap_front_df,'male':MALE_FRONT_RATIO,'female':FEMALE_FRONT_RATIO}]:
        curr_df = x['df']
        for idx, row in curr_df.iterrows():
            summary = {}
            full = {}
            
            summary.update({"healthCode":row["healthCode"],"recordId":row["recordId"],'answers.sex':row['answers.sex'],'table_label':row['table_label']})
            full.update({"healthCode":row["healthCode"],"recordId":row["recordId"],'answers.sex':row['answers.sex'],'table_label':row['table_label']})
            with open(row['path'],encoding="utf8") as f:
                 data = json.load(f)
            ctr = 0
            first_click = None
            head_clicks = 0
            torso_clicks = 0
            leg_clicks = 0

            for r in data:


                if row['answers.sex']=='Male':
                    true_height = int(r['imageWidth'])/x['male']
                    vertical_padding = (r['imageHeight']-int(r['imageWidth'])/x['male'])/2
                else:
                    true_height = int(r['imageWidth'])/x['female']
                    vertical_padding = (r['imageHeight']-int(r['imageWidth'])/x['female'])/2

                if int(r['y']) < vertical_padding or int(r['y']) > (int(r['imageHeight']) - vertical_padding):
                    click_type = 'off_screen'
                elif (int(r['y']) - vertical_padding) <= true_height/6.5:
                    click_type = 'head'
                    head_clicks = head_clicks + 1
                elif (int(r['y']) - vertical_padding) <= true_height/2.4:
                    click_type = 'torso'
                    torso_clicks = torso_clicks + 1
                else:
                    click_type = 'legs'
                    leg_clicks = leg_clicks + 1

                if ctr == 0:
                    first_click = click_type
                    full.update({'imageHeight':r['imageHeight'],'imageWidth':r['imageWidth']})

                full.update({str(ctr).zfill(3) + '_x':r['x'],str(ctr).zfill(3) + '_y':r['y']})

                #print (r, vertical_padding, click_type)
                ctr = ctr + 1

            summary.update({'bmap_first_click':first_click,'bmap_head_clicks':head_clicks,'bmap_torso_clicks':torso_clicks,'bmap_leg_clicks':leg_clicks})
            output_summary_dicts.append(summary)
            output_full_dicts.append(full)

    #print(output_full_dicts,back_output_summary_dicts)
    full_df = pd.DataFrame(sorted(output_full_dicts, key=len, reverse=True))
    summary_df = pd.DataFrame(output_summary_dicts)
    return full_df, summary_df

#function to convert pandas column to local time
def createdOn_tz_convert(x):
        return (datetime.fromtimestamp(x.createdOn/1000.0) + timedelta(hours=x.createdOnTimeZone/100)).strftime('%Y-%m-%d %H:%M:%S.%f')
        #return pd.to_datetime(x.createdOn,unit='ms').dt.tz_localize('utc').dt.tz_convert(pytz.timezone(x.createdOnTimeZone))


def merge_and_extract_enhanced_profile_and_check_in(mybplab_table_dataframes):
    enhanced_profile_data = pd.DataFrame()
    enhanced_profile_data = enhanced_profile_data.reindex(columns = ['healthCode'])
    ep_extra_data = pd.DataFrame()
    check_in_data = pd.DataFrame()
    check_in_table_list = ['AfternoonV1-v2','AfternoonV2-v2','AfternoonV3-v2','Body and Mind-v14','Morning-v12','MorningV1-v3','MorningV2-v2','MorningV3-v2','Night-v14','NightV1-v2','NightV2-v2','NightV3-v2']
    for tab in mybplab_table_dataframes:
        if "Enhance Profile" in tab["table_label"]:
            tab["dataframe"] = tab["dataframe"].rename({'uploadDate':'uploadDate:' + tab["table_label"],'answers.respect':'answers.respect:' + tab["table_label"], 'answers.nervousAndStressed':'answers.nervousAndStressed:' + tab["table_label"]},axis=1,errors="ignore")
            temp_include_merge_df = tab["dataframe"].sort_values('createdOn').drop_duplicates('healthCode',keep='last')
            #duplicates shouldn't be joined to dataset but should be retained - this way we keep all data without row multiplication
            temp_include_append_df = tab["dataframe"][~tab["dataframe"].isin(temp_include_merge_df)].drop(columns=['recordId','appVersion','phoneInfo','externalId','dataGroups','createdOn','createdOnTimeZone','userSharingScope','validationErrors','substudyMemberships','dayInStudy','rawData'],errors='ignore')
            temp_include_merge_df = temp_include_merge_df.drop(columns=['recordId','appVersion','phoneInfo','externalId','dataGroups','createdOn','createdOnTimeZone','userSharingScope','validationErrors','substudyMemberships','dayInStudy','rawData'],errors='ignore')
            enhanced_profile_data = enhanced_profile_data.merge(temp_include_merge_df,how = 'outer', on = ['healthCode'])
            ep_extra_data = ep_extra_data.append(temp_include_append_df)

        elif tab["table_label"] in check_in_table_list:
            #print(tab["table_label"])
            tab["dataframe"]["table_label"] = tab["table_label"]
            if tab["table_label"] == "Night-v14":
                tab["dataframe"]["answers.whoAreYouWith.No one"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('No one')
                tab["dataframe"]["answers.whoAreYouWith.Strangers"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Strangers')
                tab["dataframe"]["answers.whoAreYouWith.Friends"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Friends')
                tab["dataframe"]["answers.whoAreYouWith.Significant other"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Significant other')
                tab["dataframe"]["answers.whoAreYouWith.Pets"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Pets')
                tab["dataframe"]["answers.whoAreYouWith.Coworkers"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Coworkers')
                tab["dataframe"]["answers.whoAreYouWith.My children"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('My children')
                tab["dataframe"]["answers.whoAreYouWith.Other family"] = tab["dataframe"]["answers.whoAreYouWith"].str.contains('Other family')

            tab["dataframe"] = tab["dataframe"].drop(columns=['externalId','dataGroups','validationErrors','answers.whoAreYouWith','substudyMemberships','bodyMapFront_bodyMapFront.json','Intervention_Result.json','rawData','Cog_Result.json','bodyMapBack_bodyMapBack.json','BP_phone_rawdata.json','BP_watch_rawdata.json','blood_pressure_stress_recorder_bloodPressure.json','blood_pressure_watch_recorder.json','answers','answers.napsTodayPart','answers.loveThemPart','answers.loveMeTodayPart','answers.RunMode','answers.CuffMode','answers.morningEmotionStress','answers.morningEmotionInControl','answers.morningEmotionJoy','answers.morningEmotionSleepTime','answers.morningEmotionSleepTime.timezone','bodyMapFront','bodyMapBack','bodyMapBack_bodyMapBack','bodyMapFront_bodyMapFront'],errors='ignore')
            if "v12" in tab["table_label"] or "v14" in tab["table_label"]:
                tab["dataframe"]["answers.completion_sbp_offset"] = tab["dataframe"]["answers.completion_sbp_offset"] + 64
                tab["dataframe"]["answers.completion_dbp_offset"] = tab["dataframe"]["answers.completion_dbp_offset"] + 126
                tab["dataframe"] = tab["dataframe"].rename({'answers.watchDeviceName':'answers.watchname','answers.measured_with_watch':'answers.watchYN','answers.completion_stress_baseline':'answers.baseline_stress_score','answers.completion_sbp_offset':'answers.calibrationvalue_sbp','answers.completion_dbp_offset':'answers.calibrationvalue_dbp','answers.validation_sbp_result':'answers.cuffvalue_sbp','answers.validation_dbp_result':'answers.cuffvalue_dbp','answers.Output_SBP':'answers.sensorvalue_DBP','answers.Output_DBP':'answers.sensorvalue_SBP','answers.Output_HR':'answers.sensorvalue_HR'},axis=1,errors="ignore")

            check_in_data = check_in_data.append(tab["dataframe"])
            #print(len(check_in_data))
        #elif tab["table_label"] == "Background Survey-v8":
        #    tab["dataframe"].to_csv('background_survey_v8.csv',index=False)


    ep_extra_data = ep_extra_data.dropna(subset=['healthCode'])
    enhanced_profile_data  = enhanced_profile_data.append(ep_extra_data)
    check_in_data['checkinNum'] = check_in_data.groupby('healthCode')['createdOn'].rank(method='first')
    check_in_data['createdOn_local'] = check_in_data.apply(createdOn_tz_convert,axis=1)

    return enhanced_profile_data, check_in_data

def get_sexes(dataframe_dicts):
    df = [i["dataframe"] for i in dataframe_dicts if i["table_label"] == 'Background Survey-v3'][0]
    df = df[["healthCode","answers.sex","uploadDate"]]
    df = df.sort_values('uploadDate').drop_duplicates('healthCode',keep='last')
    return df

def main():
    #static mapping of data tables
    table_dicts = get_table_mapping_from_local_file('all_tables.csv')
    #comment out line below and uncomment following line with info added to not log in every time
    syn = login_to_synapse(get_synapse_credentials())
    #syn = login_to_synapse(('EMAIL HERE','PASSWORD HERE'))

    #checks for correct app versions, returns all records. Special condition to not filter out Enhnace Profile data.
    relevant_table_dicts, record_list = get_relevant_tables_and_record_list(syn, table_dicts)
    print("number of records: ", len(record_list))
    for attempt in range(10):
        try:
            print("\n***** DOWNLOADING DATA FROM SYNAPSE TABLES *****")
            mybplab_table_dataframes = get_data_from_many_tables(syn,relevant_table_dicts, record_list)
            print("\n***** DOWNLOADING JSON DATA FROM SYNAPSE *****")
            bp_json_df, bmap_front_json_df, bmap_back_json_df = download_jsons_and_assemble_metadata(syn,mybplab_table_dataframes)
            unique_sexes = get_sexes(mybplab_table_dataframes)
        except synapseclient.core.exceptions.SynapseTimeoutError:
            print('Connection lost - retrying!')
        break

    print("\n***** EXTRACTING BODYMAP DATA FROM JSONS *****")
    full_bmap_df, summary_bmap_df =  extract_bodymap_data(bmap_front_json_df.merge(unique_sexes,how='left', on = ['healthCode']),bmap_back_json_df.merge(unique_sexes,how='left', on = ['healthCode']))

    # make folders to save data in
    if not os.path.exists('data_results'):
        os.makedirs('data_results')
    if not os.path.exists('data_results/bodymap_data'):
        os.makedirs('data_results/bodymap_data')

    table_list = ('Body and Mind-v6', 'Body and Mind-v7', 'Morning-v4', 'Morning-v5', 'Night-v8', 'Night-v9')
    df_out = pd.DataFrame()
    for tab in mybplab_table_dataframes:
        if tab["table_label"] in table_list:
            tab["dataframe"]["table_label"] = tab["table_label"]
            df_out = df_out.append(tab["dataframe"])
            #print(tab["table_label"])
            #print(tab["dataframe"].columns)
    #bp_json_df.to_csv("1_0_raw_bloodpressure_jsons.csv", index=False)
    #BODYMAP
    full_bmap_df.to_csv('data_results/bodymap_data/bodymap_full_results.csv',index=False)
    summary_bmap_df.to_csv('data_results/bodymap_data/bodymap_summary_results.csv',index=False)
    df_out  = df_out.fillna({'createdOnTimeZone':0})
    df_out['createdOn_local'] = df_out.apply(createdOn_tz_convert,axis=1)
    df_out = df_out.sort_values('recordId').drop_duplicates('recordId',keep='last')
    df_out.to_csv("data_results/1_0_check_in_data.csv",index=False)

main()


