import pandas as pd
import numpy as np

# Load the large_bank_deposit_growth file into a DataFrame
df1 = pd.read_csv('data/test_set/large_banks_75%_thresholds_by_deposit_growth.csv')
df3 = pd.read_csv('data/test_set/small_banks_75%_thresholds_by_deposit_growth.csv')

# Load the total bank data into another DataFrame
df2 = pd.read_csv('data/bank_data.csv')

# Merge the DataFrames based on the "Financial_Institution_Name", "IDRSSD", "Reporting_Period_End_Date" column
merged_df1 = pd.merge(df1, df2, on=['Reporting_Period_End_Date', 'IDRSSD', 'Financial_Institution_Name'], how='left')
merged_df2 = pd.merge(df3, df2, on=['Reporting_Period_End_Date', 'IDRSSD', 'Financial_Institution_Name'], how='left')

# Specify the columns for grouping
group_columns = ['Reporting_Period_End_Date', 'IDRSSD', 'Financial_Institution_Name']

# Combine rows and keep values above zero while treating zeros as zeros
df_combined1 = merged_df1.groupby(group_columns).apply(lambda x: x.apply(lambda y: np.max(y) if np.max(y) > 0 else 0))
df_combined2 = merged_df2.groupby(group_columns).apply(lambda x: x.apply(lambda y: np.max(y) if np.max(y) > 0 else 0))

# Reset the index of the resulting DataFrame
df_combined1 = df_combined1.reset_index(drop=False)
df_combined2 = df_combined2.reset_index(drop=False)

# Remove specified columns from the DataFrame
columns_to_remove = ['total_assets', 'total_deposits', 'loan_growth', 'deposit_growth']
df_combined1 = df_combined1.drop(columns_to_remove, axis=1)
df_combined2 = df_combined2.drop(columns_to_remove, axis=1)

# Save the modified DataFrame to a new CSV file
df_combined1.to_csv('data/linear/merged_large.csv', index=False)
df_combined2.to_csv('data/linear/merged_small.csv', index=False)
