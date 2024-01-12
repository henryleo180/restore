import mysql.connector
import pandas as pd
from flask import Flask, render_template
from set_config import mysql_config

app = Flask(__name__)

# Retrieve MySQL configuration from external file
db_config = mysql_config()

# Connect to the MySQL database
def connect_to_database():
    cnx = mysql.connector.connect(user=db_config['user'], password=db_config['passwd'], host=db_config['host'], database=db_config['db'])
    cursor = cnx.cursor()
    return cnx, cursor

# Close the database connection
def close_database_connection(cnx, cursor):
    cursor.close()
    cnx.close()

# Execute a SQL query and fetch the results
def execute_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

# Get data from the database based on specified conditions
def get_data(column_name, condition, having_condition=None, exclude_condition=None):
    query = f"SELECT CAST(IDRSSD AS UNSIGNED) AS IDRSSD, {column_name} FROM bank_data WHERE {condition}"
    if exclude_condition:
        query += f" AND Financial_Institution_Name NOT LIKE '%{exclude_condition}%'"
    query += " GROUP BY IDRSSD, Financial_Institution_Name"
    if having_condition:
        query += f" HAVING {having_condition}"
    data = execute_query(cursor, query)
    return data

# Select the top 50 rows from the dataframe
def top_n(df):
    return df.head(50).copy()

# Format the dataframe based on the specified columns to $ X,000
def format_dataframe(df, columns_to_scale=[], columns_to_format_currency=[]):
    for column in columns_to_scale:
        df[column] = df[column] * 1000
    for column in columns_to_format_currency:
        df[column] = '$' + df[column].apply(lambda x: '{:,.0f}'.format(x)).astype(str)
    return df

# Calculate Return on Assets (ROA) and Return on Equity (ROE) based on the provided data
def calculate_ROA_and_ROE(data_NetIncome, data_total_assets, data_total_equity):
    # Perform necessary data merging and calculations
    df_total_assets = pd.DataFrame(data_total_assets, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalAssets'])
    df_total_equity = pd.DataFrame(data_total_equity, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalEquity'])
    df_NetIncome = pd.DataFrame(data_NetIncome, columns=['IDRSSD', 'NetIncome'])

    df_assets = pd.merge(df_total_assets, df_NetIncome, on='IDRSSD')
    df_equity = pd.merge(df_total_equity, df_NetIncome, on='IDRSSD')

    df_assets[['TotalAssets', 'NetIncome']] = df_assets[['TotalAssets', 'NetIncome']].apply(pd.to_numeric, errors='coerce')
    df_equity[['TotalEquity', 'NetIncome']] = df_equity[['TotalEquity', 'NetIncome']].apply(pd.to_numeric, errors='coerce')

    df_assets['ROA'] = (df_assets['NetIncome'] / df_assets['TotalAssets']) * 100
    df_equity['ROE'] = (df_equity['NetIncome'] / df_equity['TotalEquity']) * 100

    df_assets = df_assets.sort_values(by='ROA', ascending=False)
    df_equity = df_equity.sort_values(by='ROE', ascending=False)

    top_assets_df = df_assets.head(100).copy()
    top_equity_df = df_equity.head(100).copy()

    # Format and process the calculated data
    top_assets_df['ROA'] = top_assets_df['ROA'].apply(lambda x: '{:.2f}%'.format(x))
    top_equity_df['ROE'] = top_equity_df['ROE'].apply(lambda x: '{:.2f}%'.format(x))

    top_assets_df = top_n(top_assets_df[top_assets_df['NetIncome'] > 0])
    top_equity_df = top_n(top_equity_df[top_equity_df['NetIncome'] > 0])

    top_assets_df = format_dataframe(top_assets_df, columns_to_scale=['TotalAssets', 'NetIncome'], columns_to_format_currency=['TotalAssets', 'NetIncome'])
    top_equity_df = format_dataframe(top_equity_df, columns_to_scale=['TotalEquity', 'NetIncome'], columns_to_format_currency=['TotalEquity', 'NetIncome'])

    top_assets_df = top_assets_df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'TotalAssets': 'Total Assets', 'NetIncome': 'Net Income'})
    top_equity_df = top_equity_df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'TotalEquity': 'Total Equity', 'NetIncome': 'Net Income'})

    return top_assets_df, top_equity_df

# Calculate Equity Ratio based on the provided data
def calculate_equity_ratio(data_total_assets, data_total_equity):
    # Perform necessary data merging and calculations
    df_total_assets = pd.DataFrame(data_total_assets, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalAssets'])
    df_total_equity = pd.DataFrame(data_total_equity, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalEquity'])

    df_total_assets[['TotalAssets']] = df_total_assets[['TotalAssets']].apply(pd.to_numeric, errors='coerce')
    df_total_equity[['TotalEquity']] = df_total_equity[['TotalEquity']].apply(pd.to_numeric, errors='coerce')

    df_total = pd.merge(df_total_assets, df_total_equity, on='IDRSSD')
    df_total['EquityRatio'] = (df_total['TotalEquity'] / df_total['TotalAssets']) * 100

    df_total = df_total.sort_values(by='EquityRatio')
    top_equity_ratio_df = df_total.head(100).copy()

    # Format and process the calculated data
    top_equity_ratio_df['EquityRatio'] = top_equity_ratio_df['EquityRatio'].apply(lambda x: '{:.2f}%'.format(x))
    top_equity_ratio_df = top_n(top_equity_ratio_df)
    top_equity_ratio_df = format_dataframe(top_equity_ratio_df, columns_to_scale=['TotalAssets', 'TotalEquity'], columns_to_format_currency=['TotalAssets', 'TotalEquity'])
    top_equity_ratio_df = top_equity_ratio_df.drop('Financial_Institution_Name_y', axis=1)  # Drop the 'Financial_Institution_Name' column
    top_equity_ratio_df = top_equity_ratio_df.rename(columns={'Financial_Institution_Name_x': 'Financial Institution Name', 'TotalAssets': 'Total Assets', 'TotalEquity': 'Total Equity', 'EquityRatio': 'Equity Ratio'})

    return top_equity_ratio_df

# Calculate Non-Performing Loan (NPL) ratio based on the provided data
def calculate_NPL_ratio(data_total_loan, data_NPL):
    # Perform necessary data merging and calculations
    df_total_assets = pd.DataFrame(data_total_loan, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalLoan'])
    df_NPL = pd.DataFrame(data_NPL, columns=['IDRSSD','NPL'])
    df_total_assets[['TotalLoan']] = df_total_assets[['TotalLoan']].apply(pd.to_numeric, errors='coerce')
    df_NPL[['NPL']] = df_NPL[['NPL']].apply(pd.to_numeric, errors='coerce')
    df_total = pd.merge(df_total_assets, df_NPL, on='IDRSSD')
    df_total['NPLRatio'] = (df_total['NPL'] / df_total['TotalLoan']) * 100
    df_total = df_total.sort_values(by='NPLRatio', ascending=False)
    top_NPL_ratio_df = top_n(df_total)
    # Format and process the calculated data
    top_NPL_ratio_df['NPLRatio'] = top_NPL_ratio_df['NPLRatio'].apply(lambda x: '{:.2f}%'.format(x))
    top_NPL_ratio_df = format_dataframe(top_NPL_ratio_df, columns_to_scale=['TotalLoan', 'NPL'], columns_to_format_currency=['TotalLoan', 'NPL'])
    top_NPL_ratio_df = top_NPL_ratio_df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'TotalLoan': 'Total Loans', 'NPL': 'Non-Performing Loans', 'NPLRatio': 'NPL ratio'})

    return top_NPL_ratio_df

# Calculate Debt-to-Equity (D/E) ratio based on the provided data
def calculate_DEratio(data_total_liability, data_total_equity):
    # Perform necessary data merging and calculations
    df_total_equity = pd.DataFrame(data_total_equity, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalEquity'])
    df_total_liability = pd.DataFrame(data_total_liability, columns=['IDRSSD', 'TotalLiability'])
    df_total = pd.merge(df_total_equity, df_total_liability, on='IDRSSD')
    df_total[['TotalLiability', 'TotalEquity']] = df_total[['TotalLiability', 'TotalEquity']].apply(pd.to_numeric, errors='coerce')
    df_total['D/E ratio'] = df_total['TotalLiability'] / df_total['TotalEquity'] * 100
    df_total = df_total.sort_values(by='D/E ratio', ascending=False)
    top_DE_df = df_total.head(100).copy()
    # Format and process the calculated data
    top_DE_df['D/E ratio'] = top_DE_df['D/E ratio'].apply(lambda x: '{:.2f}%'.format(x))
    top_DE_df = top_n(top_DE_df[top_DE_df['TotalEquity'] > 0])
    top_DE_df = format_dataframe(top_DE_df, columns_to_scale=['TotalLiability', 'TotalEquity'], columns_to_format_currency=['TotalLiability', 'TotalEquity'])
    top_DE_df = top_DE_df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'TotalLiability': 'Total Liability', 'TotalEquity': 'Total Equity'})
    top_DE_df = top_DE_df[['IDRSSD', 'Financial Institution Name', 'Total Liability', 'Total Equity', 'D/E ratio']]

    return top_DE_df

# Calculate Efficiency Ratio based on the provided data
def calculate_efficiency_ratio(data_NonInterestExpense, data_TotalIncome):
    # Perform necessary data merging and calculations
    df_NonInterestExpense = pd.DataFrame(data_NonInterestExpense, columns=['IDRSSD', 'Financial_Institution_Name', 'NonInterestExpense'])
    df_TotalIncome = pd.DataFrame(data_TotalIncome, columns=['IDRSSD', 'TotalIncome'])
    df_efficiency = pd.merge(df_NonInterestExpense, df_TotalIncome, on='IDRSSD')
    df_efficiency[['NonInterestExpense', 'TotalIncome']] = df_efficiency[['NonInterestExpense', 'TotalIncome']].apply(pd.to_numeric, errors='coerce')
    df_efficiency['EfficiencyRatio'] = (df_efficiency['NonInterestExpense'] / df_efficiency['TotalIncome']) * 100
    df_efficiency = df_efficiency.sort_values(by='EfficiencyRatio', ascending=False)
    top_efficiency_df = df_efficiency.head(100).copy()
    # Format and process the calculated data
    top_efficiency_df['EfficiencyRatio'] = top_efficiency_df['EfficiencyRatio'].apply(lambda x: '{:.2f}%'.format(x))
    top_efficiency_df = top_n(top_efficiency_df)
    top_efficiency_df = format_dataframe(top_efficiency_df, columns_to_scale=['NonInterestExpense', 'TotalIncome'], columns_to_format_currency=['NonInterestExpense', 'TotalIncome'])
    top_efficiency_df = top_efficiency_df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'NonInterestExpense': 'Non-Interest Expense', 'TotalIncome': 'Total Income', 'EfficiencyRatio': 'Efficiency Ratio'})

    return top_efficiency_df

# Get yearly data based on the specified year
def get_yearly_data(year):
    # Specify conditions for quarterly and yearly data retrieval
    condition_quartly = f"STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') IN ('{year}-12-31', '{year}-09-30', '{year}-06-30', '{year}-03-31')"
    if year == '2023':
        condition_yearly = f"STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{year}-03-31'"
    else:
        condition_yearly = f"STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{year}-12-31'"

    # Get data from the database based on conditions
    data_total_assets = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON2170 = 0, RCFD2170, RCON2170)), 2) as TotalAssets", condition_quartly)
    data_total_equity = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON3210 = 0, RCFD3210, RCON3210)), 2) as TotalEquity", condition_quartly)
    total_assets_1billion = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON2170 = 0, RCFD2170, RCON2170)), 2) as TotalAssets", condition_quartly, "TotalAssets>1000000")
    total_equity_1billion = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON3210 = 0, RCFD3210, RCON3210)), 2) as TotalEquity", condition_quartly, "TotalEquity>1000000")
    total_equity_1billion_no_trust = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON3210 = 0, RCFD3210, RCON3210)), 2) as TotalEquity", condition_quartly, "TotalEquity>1000000", exclude_condition='TRUST')
    data_total_liability = get_data("ROUND(SUM(IF(RCON2948 = 0, RCFD2948, RCON2948)), 2) as TotalLiability", condition_quartly)
    total_liability_no_trust = get_data("ROUND(SUM(IF(RCON2948 = 0, RCFD2948, RCON2948)), 2) as TotalLiability", condition_quartly, exclude_condition='TRUST')
    data_NonInterestExpense = get_data("Financial_Institution_Name, AVG(RIAD4093)", condition_yearly, "AVG(RIAD4093)>0")
    data_NetIncome = get_data("AVG(RIAD4340)", condition_yearly)
    data_TotalIncome = get_data("ROUND(AVG(RIAD4079 + RIAD4107), 2) as TotalIncome", condition_yearly, "TotalIncome>0")
    data_NPL = get_data("SUM(IF((RCON1403 + RCON1407) = 0, RCFD1403 + RCFD1407, RCON1403 + RCON1407)) as NPL", condition_quartly, "NPL>0")
    data_total_loan = get_data("Financial_Institution_Name, ROUND(SUM(IF(RCON5369+RCONB529+RCON3123 = 0, RCFD5369+RCFDB529+RCFD3123, RCON5369+RCONB529+RCON3123)), 2) as TotalLoan", condition_quartly, "TotalLoan>0")

    # Calculate ratios based on the retrieved data
    top_assets_df, top_equity_df = calculate_ROA_and_ROE(data_NetIncome, data_total_assets, data_total_equity)
    top_assets_1billion, top_equity_1billion = calculate_ROA_and_ROE(data_NetIncome, total_assets_1billion, total_equity_1billion)
    top_DE_df = calculate_DEratio(data_total_liability, data_total_equity)
    top_DE_NT_df = calculate_DEratio(total_liability_no_trust, data_total_equity)
    top_efficiency_df = calculate_efficiency_ratio(data_NonInterestExpense, data_TotalIncome)
    top_equity_ratio_df = calculate_equity_ratio(data_total_assets, data_total_equity)
    top_equity_ratio_1billion = calculate_equity_ratio(total_assets_1billion, total_equity_1billion_no_trust)
    top_NPL_df = calculate_NPL_ratio(data_total_loan, data_NPL)

    return top_assets_df, top_equity_df, top_DE_df, top_efficiency_df, top_assets_1billion, top_equity_1billion, top_DE_NT_df, top_equity_ratio_df, top_equity_ratio_1billion, top_NPL_df


# Get data for multiple quarters
def select_total_deposits_multiple_dates(dates):
    dataframes = []
    for i in range(1, len(dates)):
        previous_date = dates[i]
        current_date = dates[i-1]
        
        query = f"""
        SELECT CAST(IDRSSD AS UNSIGNED) AS IDRSSD,
            Financial_Institution_Name,
            MAX(CASE WHEN STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{current_date}' THEN IF(RCON2200 = 0, RCFN2200, RCON2200) END) AS TotalDeposits_{current_date.replace('-', '_')},
            MAX(CASE WHEN STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{previous_date}' THEN IF(RCON2200 = 0, RCFN2200, RCON2200) END) AS TotalDeposits_{previous_date.replace('-', '_')}
        FROM bank_data
        WHERE STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') IN ('{current_date}', '{previous_date}')
        GROUP BY IDRSSD, Financial_Institution_Name
        HAVING (TotalDeposits_{current_date.replace('-', '_')} > 0) AND (TotalDeposits_{previous_date.replace('-', '_')} > 0)
        """

        data = execute_query(cursor, query)
        df = calculate_growth_rate(data, previous_date, current_date)
        dataframes.append(df)
    
    return dataframes


# Calculate the growth rate for a single quarter
def calculate_growth_rate(data, previous_date, current_date):
    df_deposits = pd.DataFrame(data, columns=['IDRSSD', 'Financial_Institution_Name', f'TotalDeposits_{current_date.replace("-", "_")}', f'TotalDeposits_{previous_date.replace("-", "_")}'])
    df_deposits[f'TotalDeposits_{current_date.replace("-", "_")}'] = df_deposits[f'TotalDeposits_{current_date.replace("-", "_")}'].astype(float)
    df_deposits[f'TotalDeposits_{previous_date.replace("-", "_")}'] = df_deposits[f'TotalDeposits_{previous_date.replace("-", "_")}'].astype(float)
    df_deposits['GrowthRate'] = round((df_deposits[f'TotalDeposits_{current_date.replace("-", "_")}'] - df_deposits[f'TotalDeposits_{previous_date.replace("-", "_")}']) / df_deposits[f'TotalDeposits_{previous_date.replace("-", "_")}'] * 100, 2)
    df_deposits = df_deposits.sort_values(by='GrowthRate')
    df_deposits = top_n(df_deposits)
    df_deposits['GrowthRate'] = df_deposits['GrowthRate'].apply(lambda x: '{:.2f}%'.format(x))
    df_deposits = format_dataframe(df_deposits, columns_to_scale=[f'TotalDeposits_{current_date.replace("-", "_")}', f'TotalDeposits_{previous_date.replace("-", "_")}'], columns_to_format_currency=[f'TotalDeposits_{current_date.replace("-", "_")}', f'TotalDeposits_{previous_date.replace("-", "_")}'])
    df_deposits = df_deposits.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', f'TotalDeposits_{current_date.replace("-", "_")}': f'Total Deposits {current_date}', f'TotalDeposits_{previous_date.replace("-", "_")}': f'Total Deposits {previous_date}', 'GrowthRate': 'Growth Rate'})
    
    return df_deposits

# Get data for multiple quarters
def select_total_loans_multiple_dates(dates):
    dataframes = []
    for i in range(1, len(dates)):
        previous_date = dates[i]
        current_date = dates[i-1]
        
        query = f"""
        SELECT CAST(IDRSSD AS UNSIGNED) AS IDRSSD,
            Financial_Institution_Name,
            MAX(CASE WHEN STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{current_date}' THEN IF(RCON5369+RCONB529+RCON3123 = 0, RCFD5369+RCFDB529+RCFD3123, RCON5369+RCONB529+RCON3123) END) AS TotalLoans_{current_date.replace('-', '_')},
            MAX(CASE WHEN STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{previous_date}' THEN IF(RCON5369+RCONB529+RCON3123 = 0, RCFD5369+RCFDB529+RCFD3123, RCON5369+RCONB529+RCON3123) END) AS TotalLoans_{previous_date.replace('-', '_')}
        FROM bank_data
        WHERE STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') IN ('{current_date}', '{previous_date}')
        GROUP BY IDRSSD, Financial_Institution_Name
        HAVING (TotalLoans_{current_date.replace('-', '_')} > 0) AND (TotalLoans_{previous_date.replace('-', '_')} > 0)
        """

        data = execute_query(cursor, query)
        df = calculate_loan_growth_rate(data, previous_date, current_date)
        dataframes.append(df)
    
    return dataframes


# Calculate the growth rate for loans for a single quarter
def calculate_loan_growth_rate(data, previous_date, current_date):
    df_loans = pd.DataFrame(data, columns=['IDRSSD', 'Financial_Institution_Name', f'TotalLoans_{current_date.replace("-", "_")}', f'TotalLoans_{previous_date.replace("-", "_")}'])
    df_loans[f'TotalLoans_{current_date.replace("-", "_")}'] = df_loans[f'TotalLoans_{current_date.replace("-", "_")}'].astype(float)
    df_loans[f'TotalLoans_{previous_date.replace("-", "_")}'] = df_loans[f'TotalLoans_{previous_date.replace("-", "_")}'].astype(float)
    df_loans['GrowthRate'] = round((df_loans[f'TotalLoans_{current_date.replace("-", "_")}'] - df_loans[f'TotalLoans_{previous_date.replace("-", "_")}']) / df_loans[f'TotalLoans_{previous_date.replace("-", "_")}'] * 100, 2)
    df_loans = df_loans.sort_values(by='GrowthRate', ascending=False)
    df_loans = df_loans.head(100).copy()
    df_loans['GrowthRate'] = df_loans['GrowthRate'].apply(lambda x: '{:.2f}%'.format(x))
    df_loans = top_n(df_loans)
    df_loans = format_dataframe(df_loans, columns_to_scale=[f'TotalLoans_{current_date.replace("-", "_")}', f'TotalLoans_{previous_date.replace("-", "_")}'], columns_to_format_currency=[f'TotalLoans_{current_date.replace("-", "_")}', f'TotalLoans_{previous_date.replace("-", "_")}'])
    df_loans = df_loans.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', f'TotalLoans_{current_date.replace("-", "_")}': f'Total Loans {current_date}', f'TotalLoans_{previous_date.replace("-", "_")}': f'Total Loans {previous_date}', 'GrowthRate': 'Growth Rate'})
    
    return df_loans

# Get loan-to-deposit ratio for multiple dates
def calculate_loan_to_deposit_ratio(dates):
    dataframes = []
    for date in dates:
        query = f"""
        SELECT CAST(IDRSSD AS UNSIGNED) AS IDRSSD,
            Financial_Institution_Name,
            IF(RCON5369+RCONB529+RCON3123 = 0, RCFD5369+RCFDB529+RCFD3123, RCON5369+RCONB529+RCON3123) AS TotalLoans,
            IF(RCON2200 = 0, RCFN2200, RCON2200) AS TotalDeposits,
            ROUND(IF(RCON5369+RCONB529+RCON3123 = 0, RCFD5369+RCFDB529+RCFD3123, RCON5369+RCONB529+RCON3123)/IF(RCON2200 = 0, RCFN2200, RCON2200) * 100, 2) AS LoanToDepositRatio
        FROM bank_data
        WHERE STR_TO_DATE(Reporting_Period_End_Date, '%Y-%m-%d') = '{date}'
        """

        data = execute_query(cursor, query)
        df = pd.DataFrame(data, columns=['IDRSSD', 'Financial_Institution_Name', 'TotalLoans', 'TotalDeposits', 'LoanToDepositRatio'])
        df = df.sort_values(by='LoanToDepositRatio', ascending=False)
        df = df.head(100).copy()
        # add a % sign to the end of the LoanToDepositRatio column
        df['LoanToDepositRatio'] = df['LoanToDepositRatio'].apply(lambda x: '{:.2f}%'.format(x))
        df = top_n(df)
        df = format_dataframe(df, columns_to_scale=['TotalLoans', 'TotalDeposits'], columns_to_format_currency=['TotalLoans', 'TotalDeposits'])
        df = df.rename(columns={'Financial_Institution_Name': 'Financial Institution Name', 'TotalLoans': 'Total Loans', 'TotalDeposits': 'Total Deposits', 'LoanToDepositRatio': 'LDR'})
        df.insert(0, 'Date', date)
        dataframes.append(df)
    
    return dataframes


# Connect to the database
cnx, cursor = connect_to_database()


# 6 years data
dates = ['2023-03-31', '2022-12-31', '2022-09-30', '2022-06-30', '2022-03-31', '2021-12-31', '2021-09-30', '2021-06-30', '2021-03-31', '2020-12-31', '2020-09-30', '2020-06-30', '2020-03-31', '2019-12-31', '2019-09-30', '2019-06-30', '2019-03-31', '2018-12-31', '2018-09-30', '2018-06-30', '2018-03-31']
top_deposit_df = select_total_deposits_multiple_dates(dates)
top_loan_df = select_total_loans_multiple_dates(dates)
loan_to_deposit_ratio_df = calculate_loan_to_deposit_ratio(dates)


# Get yearly data for the specified years
top_assets_df_2023, top_equity_df_2023, top_DE_df_2023, top_efficiency_df_2023, top_assets_1billion_2023, top_equity_1billion_2023, top_DE_NT_df_2023, top_er_df_2023, top_er_1billion_2023, top_NPL_2023 = get_yearly_data("2023")
top_assets_df_2022, top_equity_df_2022, top_DE_df_2022, top_efficiency_df_2022, top_assets_1billion_2022, top_equity_1billion_2022, top_DE_NT_df_2022, top_er_df_2022, top_er_1billion_2022, top_NPL_2022 = get_yearly_data("2022")
top_assets_df_2021, top_equity_df_2021, top_DE_df_2021, top_efficiency_df_2021, top_assets_1billion_2021, top_equity_1billion_2021, top_DE_NT_df_2021, top_er_df_2021, top_er_1billion_2021, top_NPL_2021 = get_yearly_data("2021")
top_assets_df_2020, top_equity_df_2020, top_DE_df_2020, top_efficiency_df_2020, top_assets_1billion_2020, top_equity_1billion_2020, top_DE_NT_df_2020, top_er_df_2020, top_er_1billion_2020, top_NPL_2020 = get_yearly_data("2020")
top_assets_df_2019, top_equity_df_2019, top_DE_df_2019, top_efficiency_df_2019, top_assets_1billion_2019, top_equity_1billion_2019, top_DE_NT_df_2019, top_er_df_2019, top_er_1billion_2019, top_NPL_2019 = get_yearly_data("2019")
top_assets_df_2018, top_equity_df_2018, top_DE_df_2018, top_efficiency_df_2018, top_assets_1billion_2018, top_equity_1billion_2018, top_DE_NT_df_2018, top_er_df_2018, top_er_1billion_2018, top_NPL_2018 = get_yearly_data("2018")

# Close the database connection
close_database_connection(cnx, cursor)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/earnings_ability')
def earnings_ability():
    ROA_2023 = top_assets_df_2023.to_html(index=False)
    ROE_2023 = top_equity_df_2023.to_html(index=False)
    ROA_2022 = top_assets_df_2022.to_html(index=False)
    ROE_2022 = top_equity_df_2022.to_html(index=False)
    ROA_2021 = top_assets_df_2021.to_html(index=False)
    ROE_2021 = top_equity_df_2021.to_html(index=False)
    ROA_2020 = top_assets_df_2020.to_html(index=False)
    ROE_2020 = top_equity_df_2020.to_html(index=False)
    ROA_2019 = top_assets_df_2019.to_html(index=False)
    ROE_2019 = top_equity_df_2019.to_html(index=False)
    ROA_2018 = top_assets_df_2018.to_html(index=False)
    ROE_2018 = top_equity_df_2018.to_html(index=False)
    ROA_1billion_2023 = top_assets_1billion_2023.to_html(index=False)
    ROE_1billion_2023 = top_equity_1billion_2023.to_html(index=False)
    ROA_1billion_2022 = top_assets_1billion_2022.to_html(index=False)
    ROE_1billion_2022 = top_equity_1billion_2022.to_html(index=False)
    ROA_1billion_2021 = top_assets_1billion_2021.to_html(index=False)
    ROE_1billion_2021 = top_equity_1billion_2021.to_html(index=False)
    ROA_1billion_2020 = top_assets_1billion_2020.to_html(index=False)
    ROE_1billion_2020 = top_equity_1billion_2020.to_html(index=False)
    ROA_1billion_2019 = top_assets_1billion_2019.to_html(index=False)
    ROE_1billion_2019 = top_equity_1billion_2019.to_html(index=False)
    ROA_1billion_2018 = top_assets_1billion_2018.to_html(index=False)
    ROE_1billion_2018 = top_equity_1billion_2018.to_html(index=False)

    return render_template('earnings_ability.html', ROA_2023=ROA_2023, ROE_2023=ROE_2023, ROA_2022=ROA_2022, ROE_2022=ROE_2022, ROA_2021=ROA_2021, ROE_2021=ROE_2021, 
                           ROA_1billion_2023=ROA_1billion_2023, ROE_1billion_2023=ROE_1billion_2023, ROA_1billion_2022=ROA_1billion_2022, ROE_1billion_2022=ROE_1billion_2022, ROA_1billion_2021=ROA_1billion_2021, ROE_1billion_2021=ROE_1billion_2021,
                            ROA_2020=ROA_2020, ROE_2020=ROE_2020, ROA_1billion_2020=ROA_1billion_2020, ROE_1billion_2020=ROE_1billion_2020, ROA_2019=ROA_2019, ROE_2019=ROE_2019, ROA_1billion_2019=ROA_1billion_2019, ROE_1billion_2019=ROE_1billion_2019,
                            ROA_2018=ROA_2018, ROE_2018=ROE_2018, ROA_1billion_2018=ROA_1billion_2018, ROE_1billion_2018=ROE_1billion_2018)

@app.route('/equity_ratio')
def equity_ratio():
    ER_2023 = top_er_df_2023.to_html(index=False)
    ER_2022 = top_er_df_2022.to_html(index=False)
    ER_2021 = top_er_df_2021.to_html(index=False)
    ER_2020 = top_er_df_2020.to_html(index=False)
    ER_2019 = top_er_df_2019.to_html(index=False)
    ER_2018 = top_er_df_2018.to_html(index=False)
    ER_1billion_2023 = top_er_1billion_2023.to_html(index=False)
    ER_1billion_2022 = top_er_1billion_2022.to_html(index=False)
    ER_1billion_2021 = top_er_1billion_2021.to_html(index=False)
    ER_1billion_2020 = top_er_1billion_2020.to_html(index=False)
    ER_1billion_2019 = top_er_1billion_2019.to_html(index=False)
    ER_1billion_2018 = top_er_1billion_2018.to_html(index=False)

    return render_template('equity_ratio.html', ER_2023=ER_2023, ER_2022=ER_2022, ER_2021=ER_2021, ER_1billion_2023=ER_1billion_2023, ER_1billion_2022=ER_1billion_2022, ER_1billion_2021=ER_1billion_2021,
                            ER_2020=ER_2020, ER_1billion_2020=ER_1billion_2020, ER_2019=ER_2019, ER_1billion_2019=ER_1billion_2019, ER_2018=ER_2018, ER_1billion_2018=ER_1billion_2018)

@app.route('/efficiency_ratio')
def efficiency_ratio():
    ER_2023 = top_efficiency_df_2023.to_html(index=False)
    ER_2022 = top_efficiency_df_2022.to_html(index=False)
    ER_2021 = top_efficiency_df_2021.to_html(index=False)
    ER_2020 = top_efficiency_df_2020.to_html(index=False)
    ER_2019 = top_efficiency_df_2019.to_html(index=False)
    ER_2018 = top_efficiency_df_2018.to_html(index=False)

    return render_template('efficiency_ratio.html', ER_2023=ER_2023, ER_2022=ER_2022, ER_2021=ER_2021, ER_2020=ER_2020, ER_2019=ER_2019, ER_2018=ER_2018)

@app.route('/debt_equity_ratio')
def debt_equity_ratio():
    DE_2023 = top_DE_df_2023.to_html(index=False)
    DE_2022 = top_DE_df_2022.to_html(index=False)
    DE_2021 = top_DE_df_2021.to_html(index=False)
    DE_2020 = top_DE_df_2020.to_html(index=False)
    DE_2019 = top_DE_df_2019.to_html(index=False)
    DE_2018 = top_DE_df_2018.to_html(index=False)
    DE_NT_2023 = top_DE_NT_df_2023.to_html(index=False)
    DE_NT_2022 = top_DE_NT_df_2022.to_html(index=False)
    DE_NT_2021 = top_DE_NT_df_2021.to_html(index=False)
    DE_NT_2020 = top_DE_NT_df_2020.to_html(index=False)
    DE_NT_2019 = top_DE_NT_df_2019.to_html(index=False)
    DE_NT_2018 = top_DE_NT_df_2018.to_html(index=False)

    return render_template('debt_equity_ratio.html', DE_2023=DE_2023, DE_2022=DE_2022, DE_2021=DE_2021, DE_NT_2023=DE_NT_2023, DE_NT_2022=DE_NT_2022, DE_NT_2021=DE_NT_2021,
                            DE_2020=DE_2020, DE_NT_2020=DE_NT_2020, DE_2019=DE_2019, DE_NT_2019=DE_NT_2019, DE_2018=DE_2018, DE_NT_2018=DE_NT_2018)

@app.route('/NPL_ratio')
def NPL_ratio():
    NPL_2023 = top_NPL_2023.to_html(index=False)
    NPL_2022 = top_NPL_2022.to_html(index=False)
    NPL_2021 = top_NPL_2021.to_html(index=False)
    NPL_2020 = top_NPL_2020.to_html(index=False)
    NPL_2019 = top_NPL_2019.to_html(index=False)
    NPL_2018 = top_NPL_2018.to_html(index=False)

    return render_template('NPL_ratio.html', NPL_2023=NPL_2023, NPL_2022=NPL_2022, NPL_2021=NPL_2021, NPL_2020=NPL_2020, NPL_2019=NPL_2019, NPL_2018=NPL_2018)

@app.route('/deposit_growth')
def deposit_growth():
    tables = [df.to_html(classes='data', header="true", index=False) for df in top_deposit_df]
    return render_template('deposit_growth.html', tables=tables)

@app.route('/loan_growth')
def loan_growth():
    tables = [df.to_html(classes='data', header="true", index=False) for df in top_loan_df]
    return render_template('loan_growth.html', tables=tables)

@app.route('/LDR')
def LDR():
    tables = [df.to_html(classes='data', header="true", index=False) for df in loan_to_deposit_ratio_df]
    return render_template('LDR.html', tables=tables)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
    