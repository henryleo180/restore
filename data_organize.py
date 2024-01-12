import glob
import pandas as pd
import os
import csv
import numpy as np

def txt_to_csv(input_directory,output_directory):
    # Get a list of all .txt files in the input directory
    txt_files = [file for file in os.listdir(input_directory) if file.endswith('.txt')]

    # Convert each .txt file to .csv
    for txt_file in txt_files:
        txt_path = os.path.join(input_directory, txt_file)
        csv_file = os.path.splitext(txt_file)[0] + '.csv'
        csv_path = os.path.join(output_directory, csv_file)

        with open(txt_path, 'r') as txt_file, open(csv_path, 'w', newline='') as csv_file:
            reader = csv.reader(txt_file, delimiter='\t')  # Assuming tab-separated values in the .txt file
            writer = csv.writer(csv_file)

            for row in reader:
                row = [item.replace('.0', '') if isinstance(item, str) else item for item in row]  # Remove '.0' from strings
                writer.writerow(row)

        print(f"Converted '{txt_file}' to '{csv_file}'")

    print("Conversion completed.")

def combine_files():
    # read data end with 1
    csv_list1 = glob.glob('./data/converted/*(1 of 2).csv')
    # read data end with 2
    csv_list2 = glob.glob('./data/converted/*(2 of 2).csv')

    csvdatadf1 = pd.DataFrame()
    csvdatadf2 = pd.DataFrame()

    # Combine the .csv files by row
    for i in csv_list1:
        csvdata1 = pd.read_csv(i)
        csvdata1 = csvdata1.iloc[1:]  # Remove the second row
        csvdatadf1 = pd.concat([csvdata1, csvdatadf1])

    for i in csv_list2:
        csvdata2 = pd.read_csv(i)
        csvdata2 = csvdata2.iloc[1:]  # Remove the second row
        csvdatadf2 = pd.concat([csvdata2, csvdatadf2])

    # combine .csv file by colume
    combined_df = pd.merge(csvdatadf1, csvdatadf2, how="outer")
    return combined_df

def process_data(df):

    # delete the duplicate columes
    df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)

    # fill the blank with 0
    df.fillna(0, inplace=True)

    # Replace ' ' with '_' in the column names
    df.columns = df.columns.str.replace(' ', '_')

    # Specify the columns you want to keep
    columns_to_keep = ['Reporting_Period_End_Date', 'IDRSSD', 'Financial_Institution_Name', 'RCON2170', 'RCFD2170', 'RCON3210', 'RCFD3210', 
                       'RCON2948', 'RCFD2948', 'RIAD4093', 'RIAD4340', 'RIAD4079', 'RIAD4107','RCON1403', 'RCON1407', 'RCFD1403', 'RCFD1407', 
                       'RCON2200', 'RCFN2200', 'RCFD5369', 'RCFDB529', 'RCFD3123', 'RCON5369', 'RCONB529', 'RCON3123' ] 

    # Keep only the desired columns
    df = df[columns_to_keep]

    return df

# def get_code_pairs():
#     df = pd.read_csv('data/code_to_name.csv')  # file that store item code and item name
#
#     df = df.merge(df, left_on=['Item_Code', 'Name'], right_on=['Item_Code', 'Name'])
#     df = df[df.code_x != df.code_y]
#     col_with_same_name = df.code_x.to_list()
#     code_pairs = np.array(col_with_same_name).reshape(len(col_with_same_name) // 2, 2)
#
#     return code_pairs
# def add_up_complemented_col(code_pairs, data):
#     # add up paired cols
#     for pair in code_pairs:
#         print(data[pair[0]])
#         print(data[pair[1]])
#         data[pair[0]] = data[pair[0]]+data[pair[1]] #.sum(axis=1)
#     data.value_counts()
#     # print(code_pairs)
#     return data
# def drop_zero_cols(df):
#     sz = df.shape[0]
#     zero_percent = 0.9
#     for col in df.columns[13:]:
#         zero_rate = df[col].value_counts() / sz
#         # print('{}: zero accounts for {:.2f}%'.format(col, zero_rate.get(0, 0) * 100, ))
#         if zero_rate.get(0, 0) > zero_percent:
#             df.drop(columns=col, inplace=True)
#     return df

def output_final_file(df):
    # output the csv
    df.to_csv("./data/bank_data.csv", index=False, sep=',')
    print(df)
    print("Save data in bank_data.csv successfully!")

if __name__ == '__main__':
    # set the path to the current path
    os.chdir(os.path.dirname(__file__))

    input_directory = 'data/downfiles'  # Specify the input directory containing the .txt files
    output_directory = 'data/converted'  # Specify the output directory to store the converted .csv files

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    txt_to_csv(input_directory, output_directory)

    output_final_file(process_data(combine_files()))
