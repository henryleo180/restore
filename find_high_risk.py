import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

def cal_indicators(df):
    # data_set = pd.DataFrame()
    data_set = df.iloc[:,:3].copy()
    data_set['total_assets'] = df['RCON2170']+df['RCFD2170']
    data_set['total_loans'] = (df['RCON5369']+df['RCFD5369'])+(df['RCONB529']+df['RCFDB529'])+(df['RCON3123']+df['RCFD3123'])
    data_set['total_deposits'] = df['RCON2200']+df['RCFN2200']
    data_set['LPR'] = data_set['total_loans'] / data_set['total_deposits']
    data_set['loan_growth'] = 0
    data_set['deposit_growth'] = 0

    IDR_lst = data_set.IDRSSD.unique()
    # len(IDR_lst)
    # calculate loan growth, deposit growth and get a final df
    df = pd.DataFrame()
    for item in IDR_lst:
        #     print(item)
        temp = data_set.loc[data_set.IDRSSD == item].sort_values(by='Reporting_Period_End_Date')
        temp['loan_growth'] = temp.total_loans / temp.total_loans.shift(1) - 1
        temp['deposit_growth'] = temp.total_deposits / temp.total_deposits.shift(1) - 1
        temp = temp.fillna(0)
        #     print(temp)
        if df.shape[0] != 0:
            df = pd.concat([df, temp])
        else:
            df = temp.copy()

    return df

def process_data(df):
    '''
    groupby primary cols and drop cols with zeros
    '''
    # drop rows that total_loans and total depoits are zeros
    result = df.drop(index=df[(df.total_loans == 0) | (df.total_deposits == 0)].index)
    result.Reporting_Period_End_Date = pd.to_datetime(result.Reporting_Period_End_Date)

    return result

def split_bank_by_size(df):
    # Split data set depend on size
    split_line = 100000000  # 250 million
    large_banks = df[df.total_assets >= split_line]
    small_banks = df[df.total_assets < split_line]
    return [large_banks, small_banks]


def screen_out_outliers(data):
    '''screen out data that is treated as outliers in each year. (outliers can be different in different years)'''
    quarters = data.Reporting_Period_End_Date.unique()
    df = data.copy()
    for quarter in quarters:
        temp = data[data.Reporting_Period_End_Date == quarter].copy()

        thirdq = [temp.loan_growth.quantile(0.75), temp.deposit_growth.quantile(0.75), temp.LPR.quantile(0.75)]
        firstq = [temp.loan_growth.quantile(0.25), temp.deposit_growth.quantile(0.25), temp.LPR.quantile(0.25)]
        screen_line_above = np.add(thirdq, np.subtract(thirdq, firstq) * 1.5)  # np.multiply(thirdq,2))
        screen_line_below = np.subtract(firstq, np.subtract(thirdq, firstq) * 5)

        outliers = temp[(temp.loan_growth > screen_line_above[0]) | (temp.loan_growth < screen_line_below[0]) |
                        (temp.deposit_growth > screen_line_above[1]) | (temp.loan_growth < screen_line_below[1]) |
                        (temp.LPR > screen_line_above[2]) | (temp.loan_growth < screen_line_below[2])].copy()
        #         print("outliers shape in {}: {}".format(quarter, outliers.shape))

        df.drop(index=outliers.index, inplace=True)

    return df.reset_index(drop=True)

def cal_avg_df(df, term=None):
    '''
    calculate the average of total_loans, total_deposits, LPR, loan_growth, deposit_growth
    df: the data set for calculating the baseline
    term: we can set terms to cal last n terms of averaged indicators
    '''
    if term:
        result = pd.DataFrame()
        for ID in df.IDRSSD.unique():
            temp = df[df.IDRSSD == ID].reset_index(drop=True)
            if len(temp.Date) >= term:
                temp_mean = temp.iloc[-term:, 1:].mean()
            else:
                temp_mean = temp.iloc[:, 1:].mean()

            result = pd.concat([result, temp_mean], axis=1)
        return result.T.reset_index(drop=True)
    else:
        return df.groupby(by='Reporting_Period_End_Date', as_index=False)[
        'total_loans', 'total_deposits', 'LPR', 'loan_growth', 'deposit_growth'].mean()


def cal_avg_diff_from_average(df, averaged_df):
    '''
    the averaged_df should be averaged indicators of no-fail bank data set, because we are calculating the
    difference percentage from the existed average.
    '''
    # averaged_df = cal_avg_df(df)
    temp = df[['Reporting_Period_End_Date', 'IDRSSD', 'LPR', 'loan_growth', 'deposit_growth']]
    temp = pd.merge(averaged_df, temp, on=['Reporting_Period_End_Date'])
    averaged_diff = pd.DataFrame()
    averaged_diff['Date'] = temp.loc[:, 'Reporting_Period_End_Date'].copy()
    averaged_diff['IDRSSD'] = temp.loc[:, 'IDRSSD'].copy()
    averaged_diff['LPR'] = np.abs(temp['LPR_y'] / temp['LPR_x'] - 1)
    averaged_diff['loan_growth'] = np.abs(temp['loan_growth_y'] / temp['loan_growth_x'] - 1)
    averaged_diff['deposit_growth'] = np.abs(temp['deposit_growth_y'] / temp['deposit_growth_x'] - 1)
    averaged_diff = averaged_diff.fillna(0)

    return averaged_diff

def get_thresholds(averaged_diff):
    '''
    The thresholds comes from failed banks, where we use the averaged diff percentages of these failed banks
    between 2018 and failed time.
    '''

    # min_thresholds = {'LPR_threshold': averaged_diff.LPR.min(), 'deposit_g_threshold': averaged_diff.deposit_growth.min(), 'loan_g_threshold': averaged_diff.loan_growth.min()}
    mean_thresholds= {'LPR_threshold': averaged_diff.LPR.mean(), 'deposit_g_threshold': averaged_diff.deposit_growth.mean(), 'loan_g_threshold': averaged_diff.loan_growth.mean()}
    thrid_quantile_thresholds = {'LPR_threshold': averaged_diff.LPR.quantile(0.75), 'deposit_g_threshold': averaged_diff.deposit_growth.quantile(0.75), 'loan_g_threshold': averaged_diff.loan_growth.quantile(0.75)}
    # max_thresholds = {'LPR_threshold': averaged_diff.LPR.max(), 'deposit_g_threshold': averaged_diff.deposit_growth.max(), 'loan_g_threshold': averaged_diff.loan_growth.max()}

    return {'mean':mean_thresholds, '75%':thrid_quantile_thresholds} #'max':max_thresholds}

def tag_high_risk_banks(thresholds, df):
    '''The input is either large banks data or small banks data.
    df: the data set with averaged indicators from last 6 terms.
    '''
    temp = df.copy()
    temp['high_risk'] = 0

    # high_risk_banks = df[df.deposit_growth >= thresholds['deposit_g_threshold']] # focus on deposit thresholds
    # df[(df.loan_growth >= thresholds['loan_g_threshold'])]
    high_risk_banks = df[(df.LPR >= thresholds['LPR_threshold'])]
    # df[(df.LPR >= thresholds['LPR_threshold']) & (df.loan_growth >= thresholds['loan_g_threshold']) &(df.deposit_growth >= thresholds['deposit_g_threshold'])]
    temp.loc[high_risk_banks.index,'high_risk'] = 1

    return temp

if __name__ == '__main__':

    raw_data = pd.read_csv('data/bank_data.csv')
    df = raw_data.groupby(['Reporting_Period_End_Date','IDRSSD','Financial_Institution_Name'],
                     axis=0, as_index=False).sum()
    print(df.shape)
    df = cal_indicators(df)
    processed_df = process_data(df)
    print(processed_df.shape)

    # separate failed banks' data from the whole data set
    failed_banks_data = pd.read_csv('data/failed_banks.csv', usecols=['IDRSSD','Financial_Institution_Name'])
    df_failed_bank = pd.merge(processed_df, failed_banks_data, on=['IDRSSD','Financial_Institution_Name'])
    df_no_fail_bank = processed_df.drop(index=processed_df[processed_df.IDRSSD.isin(failed_banks_data.IDRSSD.to_list())].index)
    # print("Shape of no-fail bank:{}, Shape of failed bank:{}".format(df_no_fail_bank.shape, df_failed_bank.shape))

    # Split both failed and no-failed bank data into large banks and small banks
    types = ['large', 'small']
    banks = split_bank_by_size(df_no_fail_bank)
    failed_banks = split_bank_by_size(df_failed_bank)

    # screen outliers from no-fail banks in order to get a reasonable average.
    large_banks = screen_out_outliers(banks[0])
    small_banks = screen_out_outliers(banks[1])
    banks = [large_banks, small_banks]
    failed_large_banks = screen_out_outliers(failed_banks[0])
    failed_small_banks = screen_out_outliers(failed_banks[1])
    failed_banks = [failed_large_banks, failed_small_banks]

    print("No-fail Bank - Shape of large bank:{}, Shape of small bank:{}".format(banks[0].shape, banks[1].shape))
    print("Failed Bank - Shape of large bank:{}, Shape of small bank:{}".format(failed_banks[0].shape, failed_banks[1].shape))

    sizes = ['large', 'small']
    for b1, b2, sz in zip(banks, failed_banks, sizes):
        # get average diff percentage of failed banks compared with the general average (baseline)
        avg_diff = cal_avg_diff_from_average(b2, cal_avg_df(b1))
        # try screen out failed bank and get a more resonable thresholds
        avg_diff['Reporting_Period_End_Date'] = avg_diff['Date']
        avg_diff = screen_out_outliers(avg_diff)
        thresholds = get_thresholds(avg_diff)

        # avg_diff of no-fail bank (large or small)
        bank_avg_diff = cal_avg_diff_from_average(b1, cal_avg_df(b1))
        bank_avg_diff_by_6_terms = cal_avg_df(bank_avg_diff, 6)
        # bank_avg_diff_by_year = bank_avg_diff.groupby(by=[bank_avg_diff.Date.dt.year,'IDRSSD'])['LPR','loan_growth','deposit_growth'].mean()

        result = {}

        for key, t in thresholds.items():
            df = tag_high_risk_banks(t, bank_avg_diff_by_6_terms)
            print("{} thresholds:\n {}".format(key, df.high_risk.value_counts()))
            df = pd.merge(b1, df[['IDRSSD','high_risk']], how='left')
            # df.to_csv("data/test_set/"+sz+"_banks_"+key+"_thresholds_by_deposit_growth.csv", index=False)
            df.to_csv("data/test_set/" + sz + "_banks_" + key + "_thresholds_by_LPR.csv", index=False)