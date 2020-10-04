import pandas as pd
import numpy as np
from hrvanalysis import get_time_domain_features, remove_outliers
import json
import os
import contextlib


file_df = pd.read_csv("file_list.txt")
file_list = file_df["path"].tolist()

def get_relevant_stats_from_raw_bp_file(file):
	with open(file, 'r+') as f:
	    content = f.read()
	    f.seek(0)
	    f.truncate()
	    f.write(content.replace('][', ','))
	with open(file) as f:
		data = json.load(f)
	rri_values = [int(d['rri']) for d in data]
	with open(os.devnull, 'w') as devnull:
		with contextlib.redirect_stdout(devnull):
			#must be between the 2 numbers
			rri_no_outliers = (remove_outliers(rri_values, 300, 2000))
	rri_no_outliers = [x for x in rri_no_outliers if str(x) != 'nan']
	if len(rri_no_outliers) > 0:
		features = get_time_domain_features(rri_no_outliers)
		feature_subset_list = ["rmssd","sdnn","mean_hr","min_hr","max_hr","std_hr"]
		feature_subset = dict((k, features[k]) for k in feature_subset_list if k in features)
	else:
		feature_subset = {}
	return feature_subset

outdicts = []
for file in file_list:
	print(file)
	outdicts.append(get_relevant_stats_from_raw_bp_file(file))

df_out = pd.DataFrame(outdicts)
df_out.to_csv("datasample.csv",index=False)



