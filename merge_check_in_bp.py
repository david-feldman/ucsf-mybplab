import pandas as pd

check_in_df = pd.read_csv("1_0_check_in_data.csv")
bp_df = pd.read_csv("processed_bp_data_1.csv")
bp_df_sub = bp_df[["recordId","rmssd","sdnn","mean_hr","min_hr","max_hr","std_hr"]]
check_in_df_out = check_in_df.merge(bp_df_sub,how='left',on = ['recordId'])
check_in_df_out.to_csv("check_in_merged_results_with_processed_bp_1_0.csv")
