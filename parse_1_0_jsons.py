import pandas as pd
import numpy as np
from hrvanalysis import get_time_domain_features, remove_outliers
import json
import os
import contextlib
import multiprocessing
from itertools import repeat


#file_df = pd.read_csv("1_0_raw_bloodpressure_jsons.csv")
#file_dicts = file_df.head().to_dict('records')
#for d in file_dicts:
#    print(get_relevant_stats_from_raw_bp_file(d))


def get_relevant_stats_from_raw_bp_file(dct, outlist):
    fl = dct["path"]
    print(fl)
    with open(fl, 'r+') as f:
        content = f.read()
        f.seek(0)
        f.truncate()
        f.write(content.replace('][', ','))
    with open(fl) as f:
        try:
            data = json.load(f)
        except:
            return
    #print(data)
    rri_values = [int(d['rri']) for d in data]
    #print(rri_values)
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull):
            #must be between the 2 numbers
            rri_no_outliers = (remove_outliers(rr_intervals=rri_values, low_rri=300, high_rri=2000))
    rri_no_outliers = [x for x in rri_no_outliers if str(x) != 'nan']
    #print(rri_no_outliers)
    if len(rri_no_outliers) > 0:
        features = get_time_domain_features(rri_no_outliers)
        feature_subset_list = ["rmssd","sdnn","mean_hr","min_hr","max_hr","std_hr"]
        feature_subset = dict((k, features[k]) for k in feature_subset_list if k in features)
    else:
        feature_subset = {}
    outlist.append({**dct,**feature_subset})

file_df = pd.read_csv("1_0_raw_bloodpressure_jsons.csv")
file_dicts = file_df.to_dict('records')
pool = multiprocessing.Pool(8)
manager = multiprocessing.Manager()
outlist = manager.list()
pool.starmap(get_relevant_stats_from_raw_bp_file, zip(file_dicts, repeat(outlist)))
pool.close()
pool.join()
df_out = pd.DataFrame(list(outlist))
df_out.to_csv("processed_bp_data.csv",index=False)
#outdicts = []

#for file in file_list:
#    print(file)
#    outdicts.append(get_relevant_stats_from_raw_bp_file(file))

#df_out = pd.DataFrame(outdicts)
#df_out.to_csv("datasample.csv",index=False)



