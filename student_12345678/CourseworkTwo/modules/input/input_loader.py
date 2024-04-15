from datetime import datetime, date
from pymongo import MongoClient
import psycopg2
import configparser
import pandas as pd

# Function to read the database configuration
def read_config(section, filename='config/script.config'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config


# Function to read data from MongoDB
def read_mongoDB():
    config = read_config('mongodb')
    client = MongoClient(config['url'])
    db = client['ExchangeTraded']
    collection = db['EquityTrades']
    documents = list(collection.find())
    df = pd.DataFrame(documents)
    return df


# Function to read data from PostgreSQL
def read_sql(table_name):
    config = read_config('postgresql')
    connection = psycopg2.connect(dbname=config['database'], user=config['user'], password=config['password'], host=config['host'], port=config['port'])
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    sql_result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    sql_result_df = pd.DataFrame(sql_result, columns=column_names)
    cursor.close()
    connection.close()
    return sql_result_df

# Get the results from MongoDB and PostgreSQL databases
mongo = read_mongoDB()
trader_limits = read_sql('cash_equity.trader_limits')
exchange_rates = read_sql('cash_equity.exchange_rates')
portfolio_positions = read_sql('cash_equity.portfolio_positions')


# Begin to load MongoDB data into PostgreSQL
# Function to generate pos_id
def generate_pos_id(row):
    date_str = row['cob_date'].strftime('%Y-%m-%d')
    trader = row['Trader']
    symbol = row['Symbol']
    ccy = row['Ccy']
    return f"{date_str}{trader}{symbol}{ccy}"


# Function to load data from MongoDB into PostgreSQL
def load_mongo_to_postgres(df):
    config = read_config('postgresql')
    connection = psycopg2.connect(dbname=config['database'], user=config['user'], password=config['password'], host=config['host'], port=config['port'])
    cursor = connection.cursor()

    # Convert MongoDB DateTime column to cob_date
    df['cob_date'] = pd.to_datetime(df['DateTime']).dt.date
    # Apply generate_pos_id to create pos_id column
    df['pos_id'] = df.apply(generate_pos_id, axis=1)
    # Group the DataFrame by pos_id and aggregate the net_quantity and net_amount
    grouped_df = df.groupby(['pos_id', 'cob_date', 'Trader', 'Symbol', 'Ccy']).agg({
        'Quantity': 'sum',   
        'Notional': 'sum'    
    }).reset_index()
    # Rename the aggregated columns
    grouped_df.rename(columns={'Quantity': 'net_quantity', 'Notional': 'net_amount'}, inplace=True)
    # Insert data into cash_equity.portfolio_positions table
    for index, row in grouped_df.iterrows():
        pos_id = row['pos_id']
        cob_date = row['cob_date']
        trader = row['Trader']
        symbol = row['Symbol']
        ccy = row['Ccy']
        net_quantity = row['net_quantity']
        net_amount = row['net_amount']
        # Construct the SQL INSERT query with schema
        sql_query = f"INSERT INTO cash_equity.portfolio_positions (pos_id, cob_date, Trader, Symbol, Ccy, net_quantity, net_amount) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        # Execute the SQL query
        cursor.execute(sql_query, (pos_id, cob_date, trader, symbol, ccy, net_quantity, net_amount))
        connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    # Read data from SQL Postgres to create DataFrame
    connection = psycopg2.connect(dbname=config['database'], user=config['user'], password=config['password'], host=config['host'], port=config['port'])
    sql_query = "SELECT * FROM cash_equity.portfolio_positions;"
    portfolio_positions = pd.read_sql(sql_query, connection)
    connection.close()

    return portfolio_positions

pp = load_mongo_to_postgres(mongo)
print(pp)


# Begin to generate policy_breaches function, with DateTime parameter
def policy_breaches(DateTime):
    mongo = read_mongoDB()
    trader_limits = read_sql('cash_equity.trader_limits')
    exchange_rates = read_sql('cash_equity.exchange_rates')

    # Convert 'DateTime' column to datetime
    mongo['DateTime'] = pd.to_datetime(mongo['DateTime'])
    # Convert datetime to yyyy-mm-dd format
    mongo['DateTime'] = mongo['DateTime'].dt.strftime('%Y-%m-%d')

    # Select rows where 'to_currency' is 'USD'
    exchange_rates = exchange_rates[exchange_rates['to_currency'] == 'USD']
    exchange_rates.drop(columns=['fx_id'], inplace=True)
    exchange_rates['cob_date'] = pd.to_datetime(exchange_rates['cob_date'])
    uer = exchange_rates[exchange_rates['cob_date'] == DateTime]
    if not uer.empty:
        exchange_rate = float(uer.loc[uer['from_currency'] == mongo['Ccy'].iloc[0], 'exchange_rate'].values[0])
        mongo['Notional'] *= exchange_rate

    # Filter rows where 'TradeType' is 'BUY' or 'SELL'
    buy_sell_mongo = mongo[(mongo['TradeType'] == 'BUY') | (mongo['TradeType'] == 'SELL')]
    grouped_mongo = buy_sell_mongo.groupby(['Trader', 'DateTime', 'Symbol', 'TradeType']).agg({'Notional': 'sum', 'Quantity': 'sum'}).reset_index()
    df = grouped_mongo.pivot_table(index=['Trader', 'DateTime', 'Symbol'], columns='TradeType', values=['Notional', 'Quantity'], fill_value=0).reset_index()
    df.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" for col in df.columns]
    df.reset_index(drop=True, inplace=True)
    df.rename(columns={'Notional_BUY': 'BuyNotional', 'Notional_SELL': 'SellNotional', 'Quantity_BUY': 'BuyQuantity', 'Quantity_SELL': 'SellQuantity'}, inplace=True)

    # Filter the DataFrame to keep rows where the 'LimitType' column is 'long', 'short', or 'volume'
    trader_limits = trader_limits[trader_limits['limit_type'].isin(['long', 'short', 'volume'])]

    # Convert 'DateTime' column to datetime.date
    mongo['DateTime'] = pd.to_datetime(mongo['DateTime']).dt.date
    date_locate = pd.DataFrame(columns=trader_limits.columns)
    for index, row in trader_limits.iterrows():
        limit_start = row['limit_start']
        limit_end = row['limit_end'] if not pd.isnull(row['limit_end']) else None
        
        if limit_end is None:
            if DateTime >= limit_start:
                date_locate = pd.concat([date_locate, pd.DataFrame(row).T], ignore_index=True)
        else:
            if limit_start <= DateTime <= limit_end:
                date_locate = pd.concat([date_locate, pd.DataFrame(row).T], ignore_index=True)

    # Select on long/short limit, begin to generate policy_breaches_1
    merged = pd.merge(df, date_locate, left_on='Trader', right_on='trader_id', how='left')
    filtered_merged = merged[merged['limit_type'].isin(['long', 'short'])]
    
    # Get the limit_amount
    pivoted_limits = filtered_merged.pivot_table(index='Trader', columns='limit_type', values='limit_amount', aggfunc='first')
    df = pd.merge(df, pivoted_limits, on='Trader', how='left')
    df.rename(columns={'long': 'longlimit', 'short': 'shortlimit'}, inplace=True)

    # Check if there is a policy breach 
    df['longbreaches'] = ['Yes' if buy_notional > long_limit else 'No' for buy_notional, long_limit in zip(df['BuyNotional'], df['longlimit'])]
    df['shortbreaches'] = ['Yes' if abs(sell_notional) > short_limit else 'No' for sell_notional, short_limit in zip(df['SellNotional'], df['shortlimit'])]
    
    # Keep the rows that at least one type that has policy breaches
    policy_breaches_1 = df[(df['longbreaches'] != 'No') | (df['shortbreaches'] != 'No')]

    # Calculate the total volume per stock per day per trader
    df['totalVolume'] = df.apply(lambda row: abs(row['BuyQuantity']) + abs(row['SellQuantity']), axis=1)

    # Calculate the total volume per stock per day
    absolute_trading_amount = df.groupby(['DateTime', 'Symbol']).apply(lambda x: (abs(x['BuyQuantity']) + abs(x['SellQuantity'])).sum())
    absolute_trading_amount = absolute_trading_amount.reset_index(name='TotalTrading')

    # Merge and calculate volume relative
    df = pd.merge(df, absolute_trading_amount, on=['Symbol', 'DateTime'], how='left')
    df['TotalTrading'].fillna(0, inplace=True)
    df['VR'] = (df['totalVolume'] / df['TotalTrading']) * 100

    # Get the limit_amount
    filtered_date_locate_volume = date_locate[date_locate['limit_type'] == 'volume']
    volumelimit_trader = filtered_date_locate_volume.groupby('trader_id')['limit_amount'].sum().reset_index()
    df = pd.merge(df, volumelimit_trader, left_on='Trader', right_on='trader_id', how='left')
    df.drop(columns=['trader_id'], inplace=True)
    df.rename(columns={'limit_amount': 'volumelimit'}, inplace=True)

    # Check if there is a policy breach 
    df['volumebreaches'] = ['Yes' if vr > volumelimit else 'No' for vr, volumelimit in zip(df['VR'], df['volumelimit'])]

    # Keep only 'Yes'
    policy_breaches_2 = df[df['volumebreaches'] == 'Yes']

    # Drop useless columns
    policy_breaches_1.drop(columns=['BuyQuantity', 'SellQuantity'], inplace=True)
    policy_breaches_2.drop(columns=['BuyNotional', 'SellNotional', 'longlimit', 'shortlimit'], inplace=True)

    return policy_breaches_1, policy_breaches_2

policy_breaches_n = policy_breaches(date(2023, 11, 23))
print(policy_breaches_n)
