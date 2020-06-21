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
      mybp_lab2_versions = ['version 2.0.0, build 116','version 2.0.3, build 120','version 2.0.5, build 122','version 2.0.6, build 124','version 2.0.7, build 125','version 2.0.8, build 126','version 2.1.0,    build 128','version 2.1.2, build 130','version 2.1.3, build 131','version 2.1.6, build 138','version 2.1.9, build 141','version 2.2.0, build 142','version 2.2.1, build 143','version 2.2.2, build 144']
      health_data_filtered = health_data[health_data["appVersion"].isin(mybp_lab2_versions)]
      relevant_table_list = health_data_filtered['originalTable'].unique().tolist()
      #tables excluded based upon offline requirements
      exclude_tables = ['WatchBaseline-v3','WatchBaseline-v4','Reminders-v1','Reminders-v2','Feedback-v1','Blood Pressure-v8','Blood Pressure-v9','Baseline-v2','Baseline-v3']
      relevant_table_list = [tab for tab in relevant_table_list if tab not in exclude_tables]
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
    #table lists based upon visually observing where relevant jsons exist in data structure
    cog_table_list = ['MorningV1-v3','NightV3-v2','AfternoonV3-v2','MorningV3-v2','NightV2-v2','AfternoonV2-v2','MorningV2-v2','NightV1-v2','AfternoonV1-v2','Night-v14','Morning-v12','Body and Mind-v14']
    int_table_list = ['AfternoonV1-v2','AfternoonV2-v2','AfternoonV3-v2','Body and Mind-v14','Morning-v12','MorningV1-v3','MorningV2-v2','MorningV3-v2','Night-v14','NightV1-v2','NightV2-v2','NightV3-v2']
    bodymap_table_list = ['Morning-v12','MorningV1-v3']
    cog_metadata_df = pd.DataFrame()
    int_metadata_df = pd.DataFrame()
    bmap_back_metadata_df = pd.DataFrame()
    bmap_front_metadata_df = pd.DataFrame()

    for d in dataframe_dicts:
        if d["table_label"] in cog_table_list:
            file_map = syn_connection.downloadTableColumns(d["synapse_table"], ['Cog_Result.json'])
            for file_handle_id, path in file_map.items():
                file_handle_dicts.append({"table_label":d["table_label"],"file_handle_id":int(file_handle_id),"path":path,"type":"Cog"})
            temp_df = d['dataframe'][['healthCode','recordId','answers.Cog_Task_Test_Name','answers.Cog_Task_Type','Cog_Result.json','answers.Cog_Test_Hits','answers.Cog_Test_Misses','answers.Cog_Test_Skips','answers.Cog_Skipped']].copy(deep=True).dropna(subset=['healthCode', 'Cog_Result.json'])
            temp_df['table_label'] = d['table_label']
            temp_df = temp_df.rename({"Cog_Result.json":"file_handle_id"},errors="raise",axis=1)
            temp_df = temp_df.astype({"file_handle_id": int,"table_label": str})
            cog_metadata_df = cog_metadata_df.append(temp_df)

        if d["table_label"] in int_table_list:
             file_map = syn_connection.downloadTableColumns(d["synapse_table"], ['Intervention_Result.json'])
             for file_handle_id, path in file_map.items():
                 file_handle_dicts.append({"table_label":d["table_label"],"file_handle_id":int(file_handle_id),"path":path,"type":"Intervention"})
             temp_df = d['dataframe'][['healthCode','recordId','answers.Intervention_Task_Group','answers.Intervention_Task_Type','Intervention_Result.json']].copy(deep=True).dropna(subset=['healthCode', 'Intervention_Result.json'])
             temp_df['table_label'] = d['table_label']
             temp_df = temp_df.rename({"Intervention_Result.json":"file_handle_id"},errors="raise",axis=1)
             temp_df = temp_df.astype({"file_handle_id":int,"table_label": str})
             #print('table_label:',d["table_label"],'raw_shape:',d['dataframe'].dropna(subset=['Intervention_Result.json']).shape,'temp_shape:',temp_df.shape)
             int_metadata_df = int_metadata_df.append(temp_df)
             #print('int_metadata_df_shape',int_metadata_df.shape)

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
    #int_metadata_df.to_csv('int_metadata_output.csv',index=False)
    file_handle_and_path_df.to_csv('all_file_handles.csv',index=False)
    cog_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'Cog'].reset_index(drop=True).merge(cog_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    int_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'Intervention'].reset_index(drop=True).merge(int_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    bmap_back_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'BodyMapBack'].reset_index(drop=True).merge(bmap_back_metadata_df,how='inner',on = ['file_handle_id','table_label'])
    bmap_front_json_output_data = file_handle_and_path_df[file_handle_and_path_df['type'] == 'BodyMapFront'].reset_index(drop=True).merge(bmap_front_metadata_df,how='inner',on = ['file_handle_id','table_label'])

    return cog_json_output_data, int_json_output_data, bmap_back_json_output_data, bmap_front_json_output_data

def get_sexes(dataframe_dicts):
    df = [i["dataframe"] for i in dataframe_dicts if i["table_label"] == 'Background Survey-v8'][0]
    df = df[["healthCode","answers.sex","uploadDate"]]
    df = df.sort_values('uploadDate').drop_duplicates('healthCode',keep='last')
    return df

def extract_bodymap_data(bmap_front_df, bmap_back_df):
    # image sample dimensions
    MALE_FRONT_RATIO = 692/ 1672
    FEMALE_FRONT_RATIO = 576/ 1672
    MALE_BACK_RATIO = 716/ 1672
    FEMALE_BACK_RATIO = 612/ 1672

    output_full_dicts = []
    output_summary_dicts = []

    for x in [{'df':bmap_back_df,'male':MALE_BACK_RATIO,'female':FEMALE_BACK_RATIO},{'df':bmap_front_df,'male':MALE_FRONT_RATIO,'female':FEMALE_FRONT_RATIO}]:
        curr_df = x['df']
        for idx, row in curr_df.iterrows():
            summary = {}
            full = {}
            summary.update({"healthCode":row["healthCode"],"recordId":row["recordId"],'answers.sex':row['answers.sex'],'table_label':row['table_label'],'type':row['type']})
            full.update({"healthCode":row["healthCode"],"recordId":row["recordId"],'answers.sex':row['answers.sex'],'table_label':row['table_label'],'type':row['type']})
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
                tab["dataframe"]["answers.completion_dbp_offset"] = tab["dataframe"]["answers.completion_dbp_offset"] + 128
                tab["dataframe"] = tab["dataframe"].rename({'answers.watchDeviceName':'answers.watchname','answers.measured_with_watch':'answers.watchYN','answers.completion_stress_baseline':'answers.baseline_stress_score','answers.completion_sbp_offset':'answers.calibrationvalue_sbp','answers.completion_dbp_offset':'answers.calibrationvalue_dbp','answers.validation_sbp_result':'answers.cuffvalue_sbp','answers.validation_dbp_result':'answers.cuffvalue_dbp','answers.Output_SBP':'answers.sensorvalue_DBP','answers.Output_DBP':'answers.sensorvalue_SBP','answers.Output_HR':'answers.sensorvalue_HR'},axis=1,errors="ignore")

            check_in_data = check_in_data.append(tab["dataframe"])
            #print(len(check_in_data))
        #elif tab["table_label"] == "Background Survey-v8":
        #    tab["dataframe"].to_csv('background_survey_v8.csv',index=False)


    ep_extra_data = ep_extra_data.dropna(subset=['healthCode'])
    enhanced_profile_data  = enhanced_profile_data.append(ep_extra_data)
    check_in_data['checkinNum'] = check_in_data.groupby('healthCode')['createdOn'].rank(method='first')
    check_in_data['createdOn'] = check_in_data.apply(createdOn_tz_convert,axis=1) 

    return enhanced_profile_data, check_in_data

def extract_and_format_cog_json_data(cog_json_df):
    anagram_dicts = []
    memory_dicts = []
    number_span_dicts = []
    color_word_dicts = []
    trails_dicts = []
    trails_condensed_dicts = []
    attention_dicts = []

    for idx, row in cog_json_df.iterrows():
        with open(row['path'],encoding="utf8") as f:
                  data = json.load(f)

        #ANAGRAM TASK
        if row["answers.Cog_Task_Type"] in ("ANAGRAM"):

            #print(data)
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp = datetime.fromtimestamp(resp["answerTimestamp"] /1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    #print(trial)
                    tmp = {}
                    tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})
                    tmp["anagram"] = trial["anagram"]
                    if "answer" in trial:
                        tmp["answer"] = trial["answer"]
                    tmp["block"] = trial["block"]
                    tmp["expectedAnswer"] = trial["expectedAnswer"]
                    tmp["isSkipped"] = trial["isSkipped"]
                    if "wrongAnswers" in trial:
                        tmp["wrongAnswers"] = trial["wrongAnswers"]
                    tmp["isPractice"] = trial["isPractice"]
                    tmp["timestampFinish"] =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["timestampStart"] =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["trialName"] = trial["trialName"]
                    tmp["trialNumber"] = trial["trialNumber"]
                    if "answer" in trial:
                        tmp["answerCode"] = int(trial["answer"] == trial["expectedAnswer"])
                    else:
                        tmp["answerCode"] = 0
                    anagram_dicts.append(tmp)


        #MEMORY TASK
        elif row["answers.Cog_Task_Type"] in ("MEMORY"):
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    #print(trial)
                    tmp = {}
                    tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})
                    if "answer" in trial:
                        tmp["answer"] = trial["answer"]
                    tmp["image"] = trial["image"]
                    tmp["word"] = trial["word"]
                    tmp["isPractice"] = trial["isPractice"]
                    tmp["timestampFinish"] =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["timestampStart"] =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["trialName"] = trial["trialName"]
                    tmp["trialNumber"] = trial["trialNumber"]
                    if "answer" in trial:
                        tmp["answerCode"] = int(fuzz.ratio(trial["answer"],trial["word"]) >= 80)
                    else:
                        tmp["answerCode"] = 0
                    memory_dicts.append(tmp)

        #NUMBER_SPAN TASK
        elif row["answers.Cog_Task_Type"] in ("NUMBER_SPAN"):
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    #print(trial)
                    tmp = {}
                    tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})

                    if "answer" in trial:
                        tmp["answer"] = trial["answer"]
                    tmp["expectedAnswer"] = trial["expectedAnswer"]
                    tmp["numberSequence"] = trial["numberSequence"]
                    tmp["isPractice"] = trial["isPractice"]
                    tmp["timestampFinish"] =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["timestampStart"] =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["trialName"] = trial["trialName"]
                    tmp["trialNumber"] = trial["trialNumber"]
                    if "answer" in trial:
                        tmp["answerCode"] = int(trial["answer"] == trial["expectedAnswer"])
                    else:
                        tmp["answerCode"] = 0
                    number_span_dicts.append(tmp)

        #COLOR_WORD TASK
        elif row["answers.Cog_Task_Type"] in ("COLOR_WORD"):
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    #print(trial)
                    tmp = {}
                    tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})

                    if "answer" in trial:
                        tmp["answer"] = trial["answer"]
                    tmp["category"] = trial["category"]
                    tmp["colorAssignment"] = trial["colorAssignment"]
                    tmp["word"] = trial["word"]
                    tmp["isPractice"] = trial["isPractice"]
                    tmp["timestampFinish"] =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["timestampStart"] =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["trialName"] = trial["trialName"]
                    tmp["trialNumber"] = trial["trialNumber"]
                    if "answer" in trial:
                        tmp["answerCode"] = int(trial["answer"] == trial["colorAssignment"])
                    else:
                        tmp["answerCode"] = 0
                    tmp["timeStamp_Diff"] = trial["timestampFinish"] - trial["timestampStart"]
                    color_word_dicts.append(tmp)


        #TRAILS TASK
        elif row["answers.Cog_Task_Type"] in ("TRAILS"):
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            tmp_condensed = {}
            tmp_condensed.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})
            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    isPractice = trial["isPractice"]
                    nodes = trial["nodes"]
                    trialNumber = trial["trialNumber"]
                    trialName = trial["trialName"]
                    trialStart =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    trialFinish =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    trialElapsedTime = trial["timestampFinish"] - trial["timestampStart"]
                    tmp_condensed["trialElapsedTime: " + trialName] =  trialElapsedTime
                    previousNodeTime = trial["timestampStart"]
                    error_free_ctr = 0
                    if "clickList" in trial.keys():
                        for node in trial["clickList"]:
                            tmp = {}
                            tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})
                            tmp.update({"isPractice":isPractice,"trialNumber":trialNumber,"trialName":trialName,"trialStart":trialStart,"trialFinish":trialFinish,"trialElapsedTime":trialElapsedTime})
                            tmp["expectedNode"] = node["expectedNode"]["name"]
                            if "hitNode" in node:
                                tmp["hitNode"] = node["hitNode"]["name"]
                            tmp["prevNodeTimestamp"] =  datetime.fromtimestamp(float(previousNodeTime)/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                            tmp["nodeTimestamp"] =  datetime.fromtimestamp(node["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                            tmp["realTimeElapsed"] =  node["timestamp"] - previousNodeTime
                            if "hitNode" in node:
                                tmp["answer"] = int(node["expectedNode"]["name"] == node["hitNode"]["name"])
                                error_free_ctr +=1
                            else:
                                tmp["answer"] = 0
                                error_free_ctr += -1000
                            trails_dicts.append(tmp)
                            previousNodeTime = node["timestamp"]
                    tmp_condensed["trialErrorFree: " + trialName] = int(error_free_ctr > 0)
                trails_condensed_dicts.append(tmp_condensed)

        #ATTENTION TASK
        elif row["answers.Cog_Task_Type"] in ("ATTENTION"):
            taskName = row["answers.Cog_Task_Test_Name"]
            taskType = row["answers.Cog_Task_Type"]
            tableLabel = row["table_label"]
            healthCode = row["healthCode"]
            recordId = row["recordId"]
            version = data["version"]
            isCanceledByClose = data["isCanceledByClose"]
            timestamp =  datetime.fromtimestamp(data["timestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
            timezone = data["timezone"]
            ttype = data["type"]

            has_started = False
            for event in data["eventList"]:
                if event["eventName"] == 'EVENT_START' and has_started is False:
                    taskStart =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    has_started = True
                elif event["eventName"] == 'EVENT_FINISH':
                    taskStop =  datetime.fromtimestamp(event['timestamp']/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')

            ctr = 0
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    if ctr == 0:
                        distractionQuestionA_answer = resp["answer"]
                        distractionQuestionA_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionA_Question = resp["question"]
                        ctr += 1
                    else:
                        distractionQuestionB_answer = resp["answer"]
                        distractionQuestionB_timestamp =  datetime.fromtimestamp(resp["answerTimestamp"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                        distractionQuestionB_Question = resp["question"]

            if 'trialResults' in data.keys():
                for trial in data["trialResults"]:
                    #print(trial)
                    tmp = {}
                    tmp.update({"taskName":taskName,"version":version,"tableLabel":tableLabel,"healthCode":healthCode,"recordId":recordId,"isCanceledByClose":isCanceledByClose,"timeStamp":timestamp,"timeZone":timezone,"taskType":taskType,"taskStart":taskStart,"taskStop":taskStop,"distractionQuestionA_answer":distractionQuestionA_answer,"distractionQuestionA_Question":distractionQuestionA_Question,"distractionQuestionA_timestamp":distractionQuestionA_timestamp,"distractionQuestionB_answer":distractionQuestionB_answer,"distractionQuestionB_Question":distractionQuestionB_Question,"distractionQuestionB_timestamp":distractionQuestionB_timestamp})

                    #answerTime isAnswerPositive    isTarget    numberOfStimuli numberOfTargets
                    tmp["answerTime"] = trial["answerTime"]
                    if "isAnswerPositive" in trial:
                        tmp["isAnswerPositive"] = trial["isAnswerPositive"]
                    tmp["isTarget"] = trial["isTarget"]
                    tmp["numberOfStimuli"] = trial["numberOfStimuli"]
                    tmp["numberOfTargets"] = trial["numberOfTargets"]
                    tmp["isPractice"] = trial["isPractice"]
                    tmp["timestampFinish"] =  datetime.fromtimestamp(trial["timestampFinish"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["timestampStart"] =  datetime.fromtimestamp(trial["timestampStart"]/1000.0, pytz.timezone(data["timezone"])).strftime('%Y-%m-%d %H:%M:%S.%f')
                    tmp["trialName"] = trial["trialName"]
                    tmp["trialNumber"] = trial["trialNumber"]
                    if "isAnswerPositive" in trial:
                        tmp["hit"] = int(trial["isAnswerPositive"] == trial["isTarget"] == True)
                    else:
                        tmp["hit"] = 0

                    if "isAnswerPositive" in trial:
                        tmp["miss"] = int(trial["isAnswerPositive"] == False and trial["isTarget"] == True)
                    else:
                        tmp["miss"] = 0

                    if "isAnswerPositive" in trial:
                        tmp["falseAlarm"] = int(trial["isAnswerPositive"] == True and trial["isTarget"] == False)
                    else:
                        tmp["falseAlarm"] = 0

                    if "isAnswerPositive" in trial:
                        tmp["correctRejection"] = int(trial["isAnswerPositive"] == False and trial["isTarget"] == False)
                    else:
                        tmp["correctRejection"] = 0

                    attention_dicts.append(tmp)

 
    anagram_output = pd.DataFrame(sorted(anagram_dicts, key=len, reverse=True))
    memory_output = pd.DataFrame(sorted(memory_dicts, key=len, reverse=True))
    number_span_output = pd.DataFrame(sorted(number_span_dicts, key=len, reverse=True))
    color_word_output = pd.DataFrame(sorted(color_word_dicts, key=len, reverse=True))
    trails_output = pd.DataFrame(sorted(trails_dicts, key=len, reverse=True))
    trails_condensed_output = pd.DataFrame(sorted(trails_condensed_dicts, key=len, reverse=True))
    attention_output = pd.DataFrame(sorted(attention_dicts, key=len, reverse=True))
    #process = psutil.Process(os.getpid())
    #print(process.memory_info().rss)

    return anagram_output, memory_output, number_span_output, color_word_output, trails_output, trails_condensed_output, attention_output


def extract_and_format_int_json_data(int_json_df):
    #ctr = 0
    #ctr2 = 0
    #ctr3 = 0
    #ctr4 = 0
    #ctr5 = 0
    #can_ctr = 0
    output_dicts = []
    #question_tracker = []
    question_list = ['Did you use audio while completing this task?','Do you typically take naps on weekdays (excluding weekends)?','How are you feeling now?','How busy is your mind right now?','How clearly were you able to just focus on the bike?','How distracted were you while completing this task?','How much did you like this task?','How much do you think this task reduced your stress?','How relaxed do you feel right now?','How sleepy are you right now?','How specific were you able to get in identifying an emotion?','How stressful is the situation you were thinking about?','How worried are you about falling asleep tonight?','List three types of technology you will use today','Name an object presented that wasn\'t the bike.','What are your top three priorities today?','What best describes how your body feels right now?','What specific emotion(s) are you feeling right now:','What type of stressful situation did you think about?']
    question_var_list = ['question_used_audio','question_weekday_naps','question_how_feeling','question_mind_busy','question_clearly_focus_bike','question_distracted','question_how_like','question_reduce_stress','question_relaxed','question_sleepy','question_emotion_specificity','question_how_stressful','question_worried_sleep','question_three_things','question_name_object','question_three_things','question_body_feels','question_which_emotions','question_stressful_situation']
    question_dict = dict(zip(question_list,question_var_list))
    for idx, row in int_json_df.iterrows():
        with open(row['path'],encoding="utf8") as f:
                  data = json.load(f)
        row_data = {}
        row_data["answers.Intervention_Task_Type"] = row["answers.Intervention_Task_Type"]
        row_data["answers.Intervention_Task_Group"] = row["answers.Intervention_Task_Group"]
        row_data["table_label"] = row["table_label"]
        row_data["healthCode"] = row["healthCode"]
        row_data["recordId"] = row["recordId"]
        #if data["isCanceledByClose"]:
    #        can_ctr += 1
        #SLEEP
        #INTRO MODULES
        if row["answers.Intervention_Task_Type"] in ("INTRO_MODULE_1_B_A","INTRO_MODULE_1_A_B","INTRO_MODULE_2_B_A","INTRO_MODULE_2_A_B"):
            row_data["interventionType"]  = data["type"]
            row_data["interventionVersion"] = data["interventionVersion"]
            ts = data["timestamp"]
            tz = pytz.timezone(data["timezone"])
            dt = datetime.fromtimestamp(ts/1000.0, tz)
            #row_data["interventionTimestamp"] = dt.strftime('%Y-%m-%d %H:%M:%S')
     #       ctr = ctr + 1
            row_data["scheduledBedtimeResult_hour"] = data['scheduledBedtimeResult']['hour']
            row_data["scheduledBedtimeResult_minutes"] = data['scheduledBedtimeResult']['minutes']
            row_data["scheduledWakeTimeResult_hour"] = data['scheduledWakeTimeResult']['hour']
            row_data["scheduledWakeTimeResult_minutes"] = data['scheduledWakeTimeResult']['minutes']
            survey_ctr = 1
            if row["answers.Intervention_Task_Type"] in ("INTRO_MODULE_1_B_A","INTRO_MODULE_1_A_B"):
                for resp in data["surveyResults"]:
                    row_data[question_dict.get(resp["question"]) ] = resp["question"]
                    #question_tracker.append({"question_name":"surveyResults_question_INTRO" +str(survey_ctr).zfill(2),"question":resp["question"]})
                    row_data[question_dict.get(resp["question"]) + "_answer"] = resp["answer"]
                    survey_ctr = survey_ctr + 1

            output_dicts.append(row_data)
        #RELEXATION TASKS
        elif row["answers.Intervention_Task_Type"] in ('RELAXATION_1','RELAXATION_2','RELAXATION_3'):
      #      ctr2 = ctr2 + 1
            row_data["interventionType"]  = data["type"]
            row_data["interventionVersion"] = data["interventionVersion"]
            ts = data["timestamp"]
            tz = pytz.timezone(data["timezone"])
            dt = datetime.fromtimestamp(ts/1000.0, tz)
            #row_data["interventionTimestamp"] = dt.strftime('%Y-%m-%d %H:%M:%S')
            survey_ctr = 1
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    row_data[question_dict.get(resp["question"])] = resp["question"]
                    #question_tracker.append({"question_name":"surveyResults_question_RELAXATION" +str(survey_ctr).zfill(2),"question":resp["question"]})
                    row_data[question_dict.get(resp["question"]) + "_answer"] = resp["answer"]
                    survey_ctr = survey_ctr + 1


            row_data["dayCount"] = data["dayCount"]
            paused = False
            pause_ts = 0
            video_start_ts = 0
            first_video_ts = 0
            video_first_started = False
            paused_millis = 0
            final_video_ts = 0
            task_started = False
            start_ts = 0
            finish_ts = 0

            for event in data["eventList"]:
                if event["eventName"] == "EVENT_VIDEO_PAUSED":
                    paused = True
                    pause_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_VIDEO_PLAYING":
                    video_start_ts = event["timestamp"]
                    if not video_first_started:
                        first_video_ts = video_start_ts
                        video_first_started  = True
                    if paused:
                        paused_millis = paused_millis + (video_start_ts - pause_ts)
                        paused = False
                elif event["eventName"] == "EVENT_VIDEO_ENDED":
                    final_video_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_START" and not task_started:
                    start_ts = event["timestamp"]
                    task_started = True
                elif event["eventName"] == "EVENT_FINISH":
                    finish_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_SKIP_TASK":
                    row_data["skippedTask"] = "EVENT_SKIP_TASK"

            if finish_ts > 0:
                row_data["videoPauseSeconds"] = paused_millis / 1000.0
                row_data["videoPlayBackSeconds"] = (final_video_ts - first_video_ts - paused_millis) / 1000.0
                row_data["totalTaskSeconds"] = (finish_ts - start_ts) / 1000.0

            output_dicts.append(row_data)
        #EXTENSION
        elif row["answers.Intervention_Task_Type"] in ('EXTENSION'):
       #     ctr3 = ctr3 + 1
            row_data["interventionType"]  = data["type"]
            row_data["interventionVersion"] = data["interventionVersion"]
            ts = data["timestamp"]
            tz = pytz.timezone(data["timezone"])
            dt = datetime.fromtimestamp(ts/1000.0, tz)
            #row_data["interventionTimestamp"] = dt.strftime('%Y-%m-%d %H:%M:%S')
            survey_ctr = 1
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    row_data[question_dict.get(resp["question"])] = resp["question"]
                    #question_tracker.append({"question_name":"surveyResults_question_EXTENSION" +str(survey_ctr).zfill(2),"question":resp["question"]})
                    row_data[question_dict.get(resp["question"]) + "_answer"] = resp["answer"]
                    survey_ctr = survey_ctr + 1


            row_data["dayCount"] = data["dayCount"]
            output_dicts.append(row_data)
        # STRESS MGMT
        # BREATHING
        elif row["answers.Intervention_Task_Type"] in ("BREATHING_CONTROL","BREATHING_EXPERIMENTAL"):
         #   ctr4 = ctr4 + 1
            row_data["interventionType"]  = data["type"]
            row_data["interventionVersion"] = data["interventionVersion"]
            ts = data["timestamp"]
            tz = pytz.timezone(data["timezone"])
            dt = datetime.fromtimestamp(ts/1000.0, tz)
            row_data["interventionTimestamp"] = dt.strftime('%Y-%m-%d %H:%M:%S')
            survey_ctr = 1
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    row_data[question_dict.get(resp["question"])] = resp["question"]
                    #question_tracker.append({"question_name":"surveyResults_question_BREATHING" +str(survey_ctr).zfill(2),"question":resp["question"]})
                    row_data[question_dict.get(resp["question"]) + "_answer"] = resp["answer"]
                    survey_ctr = survey_ctr + 1
            else:
                row_data["skippedTask"] = "EVENT_SKIP_TASK"

            paused = False
            pause_ts = 0
            video_start_ts = 0
            first_video_ts = 0
            video_first_started = False
            paused_millis = 0
            final_video_ts = 0
            task_started = False
            start_ts = 0
            finish_ts = 0

            for event in data["eventList"]:
                if event["eventName"] == "EVENT_VIDEO_PAUSED":
                    paused = True
                    pause_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_VIDEO_PLAYING":
                    video_start_ts = event["timestamp"]
                    if not video_first_started:
                        first_video_ts = video_start_ts
                        video_first_started  = True
                    if paused:
                        paused_millis = paused_millis + (video_start_ts - pause_ts)
                        paused = False
                elif event["eventName"] == "EVENT_VIDEO_ENDED":
                    final_video_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_START" and not task_started:
                    start_ts = event["timestamp"]
                    task_started = True
                elif event["eventName"] == "EVENT_FINISH":
                    finish_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_SKIP_TASK":
                    row_data["skippedTask"] = "EVENT_SKIP_TASK"


            if finish_ts > 0:
                row_data["videoPauseSeconds"] = paused_millis / 1000.0
                row_data["videoPlayBackSeconds"] = (final_video_ts - first_video_ts - paused_millis) / 1000.0
                row_data["totalTaskSeconds"] = (finish_ts - start_ts) / 1000.0


            output_dicts.append(row_data)

        # OTHER STRESS MGMT
        elif row["answers.Intervention_Task_Type"] in ("EMOTION_LABELING_CONTROL","EMOTION_LABELING_EXPERIMENTAL","GOAL_SETTING_CONTROL","GOAL_SETTING_EXPERIMENTAL","TIME_DISTANCING_CONTROL","TIME_DISTANCING_EXPERIMENTAL"):
        #    ctr5 = ctr5 + 1
            row_data["interventionType"]  = data["type"]
            row_data["interventionVersion"] = data["interventionVersion"]
            ts = data["timestamp"]
            tz = pytz.timezone(data["timezone"])
            dt = datetime.fromtimestamp(ts/1000.0, tz)
            row_data["interventionTimestamp"] = dt.strftime('%Y-%m-%d %H:%M:%S')
            survey_ctr = 1
            if 'surveyResults' in data.keys():
                for resp in data["surveyResults"]:
                    row_data[question_dict.get(resp["question"])] = resp["question"]
                    #question_tracker.append({"question_name":"surveyResults_question_" +row["answers.Intervention_Task_Type"].replace('_CONTROL','').replace('_EXPERIMENTAL','') +str(survey_ctr).zfill(2), "question":resp["question"]})
                    row_data[question_dict.get(resp["question"]) + "_answer"] = resp["answer"]
                    survey_ctr = survey_ctr + 1


            paused = False
            pause_ts = 0
            video_start_ts = 0
            first_video_ts = 0
            video_first_started = False
            paused_millis = 0
            final_video_ts = 0
            task_started = False
            start_ts = 0
            finish_ts = 0

            for event in data["eventList"]:
                if event["eventName"] == "EVENT_VIDEO_PAUSED":
                    paused = True
                    pause_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_VIDEO_PLAYING":
                    video_start_ts = event["timestamp"]
                    if not video_first_started:
                        first_video_ts = video_start_ts
                        video_first_started  = True
                    if paused:
                        paused_millis = paused_millis + (video_start_ts - pause_ts)
                        paused = False
                elif event["eventName"] == "EVENT_VIDEO_ENDED":
                    final_video_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_START" and not task_started:
                    start_ts = event["timestamp"]
                    task_started = True
                elif event["eventName"] == "EVENT_FINISH":
                    finish_ts = event["timestamp"]
                elif event["eventName"] == "EVENT_SKIP_TASK":
                    row_data["skippedTask"] = "EVENT_SKIP_TASK"



            if finish_ts > 0:
                row_data["videoPauseSeconds"] = paused_millis / 1000.0
                row_data["videoPlayBackSeconds"] = (final_video_ts - first_video_ts - paused_millis) / 1000.0
                row_data["totalTaskSeconds"] = (finish_ts - start_ts) / 1000.0

            if 'followUpResults' in data.keys() and len(list(filter(None,data['followUpResults']))) > 0:
                follow_up_ctr = 1
                for result in list(filter(None,data['followUpResults'])):
                    row_data[question_dict.get(result["question"])] = result["question"]
                    #question_tracker.append({"question_name":"followUpResults_question_" + row["answers.Intervention_Task_Type"].replace('_CONTROL','').replace('_EXPERIMENTAL','')  +str(follow_up_ctr).zfill(2), "question":result["question"]})
                    local_ctr = 1
                    for an in result["answer"]:
                        row_data[question_dict.get(result["question"]) + "_answer_"  + str(local_ctr).zfill(2)] = an["userAnswer"]
                        local_ctr = local_ctr + 1
                    follow_up_ctr = follow_up_ctr + 1



            output_dicts.append(row_data)

    int_results_df = pd.DataFrame(sorted(output_dicts, key=len, reverse=True))
    return int_results_df


# main method
def main():
    #static mapping of data tables
    table_dicts = get_table_mapping_from_local_file('all_tables.csv')
    #comment out line below and uncomment following line with info added to not log in every time
    syn = login_to_synapse(get_synapse_credentials())
    #syn = login_to_synapse(('EMAIL HERE','PASSWORD HERE'))

    #checks for correct app versions, returns all records. Special condition to not filter out Enhnace Profile data.
    relevant_table_dicts, record_list = get_relevant_tables_and_record_list(syn, table_dicts)

    for attempt in range(10):
        try:
            print("\n***** DOWNLOADING DATA FROM SYNAPSE TABLES *****")
            mybplab_table_dataframes = get_data_from_many_tables(syn,relevant_table_dicts, record_list)

            #create_tables_and_columns_csv(mybplab_table_dataframes)
            print("\n***** DOWNLOADING JSON DATA FROM SYNAPSE *****")
            cog_json_df, int_json_df, bmap_front_json_df, bmap_back_json_df  = download_jsons_and_assemble_metadata(syn,mybplab_table_dataframes)
            unique_sexes = get_sexes(mybplab_table_dataframes)
        except synapseclient.core.exceptions.SynapseTimeoutError:
            print('Connection lost - retrying!')
        break

    print("\n***** EXTRACTING BODYMAP DATA FROM JSONS *****")
    full_bmap_df, summary_bmap_df =  extract_bodymap_data(bmap_front_json_df.merge(unique_sexes,how='left', on = ['healthCode']),bmap_back_json_df.merge(unique_sexes,how='left', on = ['healthCode']))

    print("\n***** EXTRACTING INTERVENTION DATA FROM JSONS *****")
    int_results_df = extract_and_format_int_json_data(int_json_df)

    print("\n***** EXTRACTING COG DATA FROM JSONS *****")
    anagram_output, memory_output, number_span_output, color_word_output, trails_output, trails_condensed_output, attention_output = extract_and_format_cog_json_data(cog_json_df)

    print("\n***** MERGING & FORMATTING CHECK-IN & EP TABLES *****")
    enhanced_profile_data, check_in_data = merge_and_extract_enhanced_profile_and_check_in(mybplab_table_dataframes)


    print("\n***** WRITING OUTPUT CSV FILES *****")

    # make folders to save data in
    if not os.path.exists('data_results'):
        os.makedirs('data_results')
    if not os.path.exists('data_results/bodymap_data'):
        os.makedirs('data_results/bodymap_data')
    if not os.path.exists('data_results/check_in_background_and_ep_data'):
        os.makedirs('data_results/check_in_background_and_ep_data')
    if not os.path.exists('data_results/check_in_background_and_ep_data/standalone_ep_tables'):
        os.makedirs('data_results/check_in_background_and_ep_data/standalone_ep_tables')
    if not os.path.exists('data_results/cog_task_data'):
        os.makedirs('data_results/cog_task_data')
    if not os.path.exists('data_results/intervention_task_data'):
        os.makedirs('data_results/intervention_task_data')
    if not os.path.exists('data_results/intervention_task_data/standalone_intervention_data'):
        os.makedirs('data_results/intervention_task_data/standalone_intervention_data')


# write data to folders
#BODYMAP
    full_bmap_df.to_csv('data_results/bodymap_data/bodymap_full_results.csv',index=False)
    summary_bmap_df.to_csv('data_results/bodymap_data/bodymap_summary_results.csv',index=False)

#INTERVENTION
    int_results_df.to_csv('data_results/intervention_task_data/intervention_results.csv',index=False)
    for int_type in int_results_df.interventionType.unique():
        int_results_df[int_results_df["interventionType"] == int_type].to_csv('data_results/intervention_task_data/standalone_intervention_data/'+int_type+'.csv',index=False)

#COG
    anagram_output.to_csv('data_results/cog_task_data/anagram_task_results.csv',index=False)
    memory_output.to_csv('data_results/cog_task_data/memory_task_results.csv',index=False)
    number_span_output.to_csv('data_results/cog_task_data/number_span_task_results.csv',index=False)
    color_word_output.to_csv('data_results/cog_task_data/color_word_task_results.csv',index=False)
    trails_output.to_csv('data_results/cog_task_data/trails_task_results.csv',index=False)
    trails_condensed_output.to_csv('data_results/cog_task_data/trails_task_condensed_results.csv',index=False)
    attention_output.to_csv('data_results/cog_task_data/attention_task_results.csv',index=False)

#EP & CHECK-IN
    enhanced_profile_data.to_csv('data_results/check_in_background_and_ep_data/enhanced_profile_merged_results.csv',index=False)
    check_in_data.to_csv('data_results/check_in_background_and_ep_data/check_in_merged_results.csv',index=False)
    for tab in mybplab_table_dataframes:
        if "Enhance Profile" in tab["table_label"]:
            tab["dataframe"]['createdOn'] = tab["dataframe"].apply(createdOn_tz_convert,axis=1) 
            tab["dataframe"].to_csv("data_results/check_in_background_and_ep_data/standalone_ep_tables/"+tab["table_label"]+".csv", index=False)
        elif "Background Survey-v8" == tab["table_label"]:
            tab["dataframe"] = tab["dataframe"].fillna({'createdOnTimeZone':0})
            tab["dataframe"]['createdOn'] = tab["dataframe"].apply(createdOn_tz_convert,axis=1)
            tab["dataframe"].to_csv("data_results/check_in_background_and_ep_data/"+tab["table_label"]+".csv", index=False)

    print("\n***** FINSHED WRITING OUTPUT CSV FILES *****")


main()


