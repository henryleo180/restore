import pandas as pd
from sklearn.linear_model import LinearRegression

df1 = pd.read_csv('data/linear/merged_large.csv')
df2 = pd.read_csv('data/linear/merged_small.csv')

def features_by_linear(df):
    # Separate the features (independent variables) from the target variable (high risk):
    X = df.drop(['Reporting_Period_End_Date', 'Financial_Institution_Name', 'high_risk', 'IDRSSD', 'LPR'],
                axis=1)  # Features
    y = df['high_risk']  # Target variable

    # Initialize and fit a linear regression model:
    model = LinearRegression()
    model.fit(X, y)

    # Retrieve the coefficients and corresponding features:
    coefficients = model.coef_
    features = X.columns

    # Create a DataFrame to display the coefficients and their corresponding features:
    coefficients_df = pd.DataFrame({'Feature': features, 'Coefficient': coefficients})

    # Sort the DataFrame by absolute coefficient values to identify the most important features:
    coefficients_df['Absolute_Coefficient'] = abs(coefficients_df['Coefficient'])
    coefficients_df = coefficients_df.sort_values('Absolute_Coefficient', ascending=False)

    # list all the output features in order
    feature_list = list(coefficients_df['Feature'])

    return feature_list


def get_top_features(n, feature_list):
    # pick the top n that is not same
    finallist = []
    count = 0
    for item in feature_list:
        if item != 'total_loans':
            item = item[4:]
        if item not in finallist:
            finallist += [item]
            count += 1
        if count == n:
            break
    return finallist


def print_final_message(finallist, banksize):
    # Read the CSV file, skipping the first row
    df_explain = pd.read_csv('data/code_to_name.csv')
    df_explain = df_explain.drop(['Mnemonic', 'code'], axis=1)
    new_row = pd.DataFrame({'Item_Code': ['total_loans'],
                            'Name': ['TOTAL LOANS']})
    df_explain = df_explain.append(new_row, ignore_index=True)

    # Sort the DataFrame based on the order of the finallist
    df_explain = df_explain[df_explain['Item_Code'].isin(finallist)]
    df_explain['Order'] = df_explain['Item_Code'].map({code: index for index, code in enumerate(finallist)})
    df_explain = df_explain.sort_values('Order')
    df_explain = df_explain.drop('Order', axis=1)
    df_explain = df_explain.drop_duplicates()

    # Extract the item names in the same order as finallist
    item_names = df_explain['Name'].tolist()

    # Print the top five features
    print("The top " + str(len(finallist)) + " features that leads to the failure of the " + banksize + " banks are:")
    for feature in item_names:
        print(feature)


features_large = features_by_linear(df1)
features_small = features_by_linear(df2)
top_five_large = get_top_features(5, features_large)
top_five_small = get_top_features(5, features_small)
print_final_message(top_five_large, 'large')
print()
print_final_message(top_five_small, 'small')
