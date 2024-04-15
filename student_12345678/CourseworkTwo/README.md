# My second coursework

- [My first coursework](#my-first-coursework)
  - [Introduction](#introduction)

  - [Business Case, Challenges, and Data](#business-case-challenges-and-data)

  - [Solution to the Challenge](#solution-to-the-challenge)
   -[Approach](#approach)
   -[Reasons for Limit Types Choosing ](#reasons-for-limit-types-choosing)
   -[Implementation](#implementation)
   -[Results](#results)
  - [Conclusion](#conclusion)

## Introduction

The data pipeline, which is also a systematic data framework, is designed to deal with real-world equity trading data more efficiently and gives traders, investors, and policymakers insights into various decisions, such as policy breaches and investment decisions.
Equity trading data nowadays has to be increasingly important to process, with more and more data pouring in all the time. Also, it is crucial for all market participants to have a solid system in place to handle it all. In addition, traders face the risks of, for example, overtrading, not as prudent, etc., which will potentially expose them to unnecessary losses. Therefore, it is necessary to monitor and control the risks in a second-line defence on the corresponding equity risk positions. This report aims to introduce a policy-breaching detected technic, to find if the traders are violating the regulated limitation on the given trade actions. There are several limits set, and this designed data pipeline will mainly focus on two limits, which are the long/short limit amount and the volume relative percentage. 
The report will introduce a self-designed data pipeline, which aims to demonstrate a method to find policy-breaching trades on a daily basis. The data pipeline will be executed in Python, connecting MongoDB and SQL database. The report will be divided into the following three sections. Firstly, the report will introduce the business case and data more comprehensively, and also identify any challenges that may be faced during the scenario itself and the whole design process. The second part will present the solution to the challenge, including the methodology and critical analyses of the applied approach, the implementation, which represents the whole design process, and the code execution result in Python. The last part will be a brief conclusion, summarising the findings and implications of the implemented data pipeline.



## Business Case, Challenges, and Data

The overall business case is focused on risk policy breaches and the main check policy limits are long/short consideration, and volume relative. Through the whole process, all the given trades can be stored in the database, and market participants can be directly given information about which single trade violated the set risk policies and its trader on a given day. 
The trading limits are typically set to recognise and control the risk positions as mentioned in the report introduction. To be more specific, long/short consideration limits the notional trading amount for different traders for a single stock position. It represents the maximum amount in USD. The trading volume relative limits the max position on the given daily volume. It is represented by the percentage (PCG), and the trader cannot have more than X% of daily volume as the ratio of position consideration. Also, the volume relative can be interpreted as a stock’s net trading quantity per trader divided by its net trading quantity.
The challenges that arise when designing and executing the data pipeline are that, firstly, the data pipeline should be able to deal with any given daily changeable trade data, potentially suggesting the necessity of complexity and logic. The policy breaching data pipeline will achieve this by parameterising the date time. Secondly, retrieving data from different types of databases requires different compatible formats, for example, MongoDB only accepts date time in a certain format, which is completely different from SQL date format, the data pipeline should be able to convert those situations to a common format. Finally, the logic behind the overall data pipeline is super important since it should allow users to only run one file to execute the whole pipeline and get the corresponding results.  
The applied MongoDB database records all end-of-day trades for all traders. The MongoDB database is called ExchangTraded, and the subordinated collection is called EquityTrades, with DateTime, TraderId, Trader, Symbol, Quantity, Notional, TradeType, Ccy, and Counterparty. The DateTime is the Mongo ISODate, the TradeType is buy or sell, and ‘Ccy’ stands for the currency of the notional amount.
The applied SQL database has six tables in the schema called ‘cash_equity’. The necessary tables that will be used in the risk policy breach data pipeline are the following:
•	Table ‘exchange_rates’ summaries the daily exchange rate across different currencies. The columns are from_currency, to_currency, exchange_rate, cob_date, and fx_id. In the designed data pipeline, the exchange rates table will be applied to uniform the currencies for comparison.
•	Table ‘trader_limits’ lists the traders, and their trade limit types, categories, and amounts with limit date periods. The data pipeline is going to pick two of the trade limit types in the given limit data periods and compare the limit_amount to the actual amount. 
•	Table ‘portfolio_positions’ provides statistics for the portfolio position changes on 2023-10-27, with trader information, net quantity, and net amount for each position change. 

## Solution to the Challenge

The reason why the long/short consideration exists is that it makes the trader prudent on the position compulsorily to some extent. Furthermore, it detects whether the trader is properly hedging or not, since ideally, long and short positions can be the same amount. 
The reason why the volume relative limit exists is that it prevents market manipulation, especially in some cases like high-frequency trades. It also helps keep market transparency and protects the financial market from dominating trading activity.

### Approaches

Firstly, the data pipeline retrieves the data from PostgreSQL (trader_limits, exchange_rates, portfolio_positions) and MongoDB (including all end-of-day trade information). 

```

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

```

Secondly, the data pipeline loads the MongoDB data into PostgreSQL, updating the table portfolio_positions for the given date.

```
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

```

Thirdly, the data pipeline checks the policy breaches using functions. For the long/short considerations, the function ‘policy_breaches_1’ is constructed as the following steps:
•	Step 1: Put the MongoDB DateTime in the right limit period and retrieve the corresponding limit information, using the given limit start and limit end columns. 
•	Step 2: Match the MongoDB DateTime with the cob_date in the exchange rates table, where let the from currency be the MongoDB ‘Ccy’ and to currency be ‘USD’, which aligns with the limit amount currency.
•	Step 3: Aggregate the total notional amount and quantity by Symbol and TradeType.
•	Step 4: Check for policy breaches to figure out if the total notional amount in USD exceeds the limit amount for the given date.
For the volume relative, the function ‘policy_breaches_2’ is constructed as the following steps:
•	Step 1: Put the MongoDB DateTime in the right limit period and retrieve the corresponding limit information, using the given limit start and limit end columns just like above.
•	Step 2: Calculate the total volume per stock by summing all the quantities that are grouped by symbol and calculate the total volume for each trader per stock by summing all the quantities that are grouped by symbol and trader. 
•	Step 3: Calculate the volume relative (%) using the total volume for each trader per stock divided by the total volume per stock.
•	Step 4: Check for policy breaches to figure out if the volume relative (%) exceeds the limit amount for the given date.

Finally, the Main.py is able to execute the data pipeline and output the policy breaches for further analysis, and then load the results into the newly-created tables in PostgreSQL.

### Reasons for Limit Types Choosing 

Compared with other policy limits, long/short limits are more important, as ‘considerations’ can be more intuitive and more accessible. It is also a more direct measure of the trader’s exposure to the market, and more flexible to a uniform change according to the market overall conditions. For instance, criteria on volatilities might fluctuate over time and require more nuanced analysis compared with a long/short amount of consideration, which immediately triggers action as long as the amounts are above the thresholds.
In addition, the approach will show the importance of exchange-traded, which will involve exchange rates in consideration. The daily exchange rates across different currencies vary, but the total limit amount in USD is comparably fixed to an integer, which allows traders to analyse and decide more cautiously. 
The volume relative limit is also simple to calculate and intuitive to interpret. It directly limits market manipulation when someone is trying to manipulate the market through high-volume trades. Also, it makes sure that the trading activity remains aligned with the capacity of the market to absorb it. Therefore, traders, investors, and policymakers need to monitor and manage the risk through limiting volume relative ratios. 
For the overall data pipeline, the chosen two limit categories are categorised as consideration and relative methods. As a result, the data pipeline that combines those different limit categories allows traders, investors, and policymakers to monitor the risks more comprehensively. 

### Implementation

The whole data pipeline is structured under the folder CourseworkTwo, including several subordinated folders called config, modules, static, test, and Main.py. The config folder contains three files: docker.config, script.config, and script.params. The configurations record basic information about PostgreSQL and MongoDB, including the database, host, collection names, etc., that can be amended without affecting overall code logic. The modules folder contains files that are used to create new tables in PostgreSQL, connect databases, load the MongoDB data into PostgreSQL, define policy_breaches function, etc. Finally, the Main.py, which is the most crucial one, links all the necessary functions. When running the Main.py, the data pipeline can execute across different databases. 

### Results

The output can be recorded into the tables that are newly created in PostgreSQL, under Schema cash_equity, named policy_breaches_1 and policy_breaches_2. These two tables are created using CreateTable.py. Six traders and six counterparties were involved, which can be further detailed under script.params file. After proceeding with the model testing, the result table will contain trade actions that occurred on 2023-11-23.  
For the long/short-consideration limits, I generated new columns based on the retrieved data frame to compare and conclude if there is a long-limit breach or a short-limit breach. If the output text in the columns shows at least one ‘Yes’, then record this trade as the policy-breaching trade. There are 21 rows output during the process of long/short policy breaching limit checking. 
For the volume relative limit, I generated new columns based on the retrieved data frame to compare and conclude if there is a volume breach. Just like above, if the output shows ‘Yes’, then record this trade. There are 44 rows output during volume breaching limit checking.
When analysing the results, it can monitor the risk of each trader, indirectly reflecting the trader’s performance. Also, at the same time, it can provide certain risk reminders to investors and policymakers. For example, for the final output table policy_breaches_1 and policy_breaches_2, we can check how many times each trader violates the policy. The results show that, for the long/short limit, MRH5231 has breached the policy the most, with 11 times, and for the volume limit, this trader also breaches the policy the most with 31 times. Thus, we should pay more attention to trader MRH5231 on the risk of overhedging and overtrading. 
In addition, as DateTime is recorded as a parameter in the policy_breaches function, the users of this data pipeline can directly change the date by applying policy_breaches(date(yyyy, mm, dd)) in Main.py, which is more flexible. 


## Conclusion

In conclusion, the development and implementation of the policy-breaching data pipeline represent the enhancement of risk management within equity trading. By dealing with a systematic approach to identify and analyse policy breaches related to long/short limit amounts and volume relative percentages, the pipeline provides traders, investors, and policymakers with valuable insights into potential risks and areas.
The results of the pipeline testing reveal several key findings. The identification of policy breaches highlights areas of concern, particularly regarding overhedging and overtrading behaviours among traders. For instance, trader MRH5231 emerged as a significant ‘outlier’, breaching both the long/short limit and volume relative limit multiple times. This suggests a higher risk of trading actions on this trader. 
Moreover, the flexibility of the pipeline demonstrated through its parameterisation of DateTime, enhances usability and adaptability to changing market conditions. The results indicate the importance of monitoring trader behaviour and adherence to established risk policies, particularly in mitigating the risks of overtrading and market manipulation, and the policy breaching data pipeline can be further improved by checking more limit types, such as ES, Volatility, etc. 
