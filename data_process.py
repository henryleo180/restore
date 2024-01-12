import pandas as pd
import numpy as np

def get_code_pairs():
    df = pd.read_csv('data/code_to_name.csv')  # file that store item code and item name
    # drop cols with str
    df.drop(index=df[df['Item_Code'] == '6724'].index, inplace=True)

    df = df.merge(df, left_on=['Item_Code', 'Name'], right_on=['Item_Code', 'Name'])
    df = df[df.code_x != df.code_y]
    col_with_same_name = df.code_x.to_list()
    code_pairs = np.array(col_with_same_name).reshape(len(col_with_same_name) // 2, 2)

    return code_pairs
def add_up_complemented_col(code_pairs, data):
    # add up paired cols
    for pair in code_pairs:
        data[pair[0]] = data[pair[0]]+data[pair[1]]

    return data

def drop_zero_cols(df):
    sz = df.shape[0]
    zero_percent = 0.9
    for col in df.columns[13:]:
        zero_rate = df[col].value_counts() / sz
        # print('{}: zero accounts for {:.2f}%'.format(col, zero_rate.get(0, 0) * 100, ))
        if zero_rate.get(0, 0) > zero_percent:
            df.drop(columns=col, inplace=True)
    return df

def group_cols(df):
    print(df.shape)
    result = df.groupby(['Reporting_Period_End_Date', 'IDRSSD','Financial_Institution_Name'], axis=0, as_index=False).sum()
    print(result.shape)

get_code_pairs()
data = pd.read_csv('data/bank_data.csv',low_memory=False)
group_cols(data)
# final_data = drop_zero_cols(add_up_complemented_col(get_code_pairs(), data))
# final_data.to_csv("./data/data_set.csv", index=False)