from datetime import datetime, date
from pymongo import MongoClient
import psycopg2
import configparser
import pandas as pd
from modules.db.SQL.CreateTable import connect, generate_id, create_tables
from modules.input.input_loader import read_mongoDB, read_sql, load_mongo_to_postgres, policy_breaches

def main():
    # Get data from MongoDB and PostgreSQL
    mongo = read_mongoDB()
    trader_limits = read_sql('cash_equity.trader_limits')
    exchange_rates = read_sql('cash_equity.exchange_rates')
    portfolio_positions = read_sql('cash_equity.portfolio_positions')

    # Get the updated portfolio_positions
    print(portfolio_positions)

    # Run policy breaches analysis
    policy_breaches_1, policy_breaches_2 = policy_breaches(date(2023, 11, 23))
    print(policy_breaches_1)
    print(policy_breaches_2)

    # Count the occurrences of "Yes" in 'longbreaches' and 'shortbreaches' columns for each trader
    long_breaches_count = policy_breaches_1[policy_breaches_1['longbreaches'] == 'Yes'].groupby('Trader').size()
    short_breaches_count = policy_breaches_1[policy_breaches_1['shortbreaches'] == 'Yes'].groupby('Trader').size()
    # Combine the counts
    total_breaches_count = long_breaches_count.add(short_breaches_count, fill_value=0)
    # Print the count of breaches for each trader
    print("Trader Breaches in policy_breaches_1:")
    print(total_breaches_count)

    # Count the occurrences of "Yes" in 'volumebreaches' column for each trader
    volume_breaches_count = policy_breaches_2[policy_breaches_2['volumebreaches'] == 'Yes'].groupby('Trader').size()
    # Print the count of breaches for each trader in policy_breaches_2
    print("Trader Breaches in policy_breaches_2:")
    print(volume_breaches_count)

    # Begin to update the policy_breaches_1 and policy_breaches_2 tables in PostgreSQL
    connection = connect()
    if connection:
        cursor = None 
        try:
            # Create tables if they do not exist
            create_tables(connection)
            
            # Initialize cursor inside try block
            cursor = connection.cursor()

            # Insert data into policy_breaches_1
            for index, row in policy_breaches_1.iterrows():
                id_value = generate_id(row['DateTime'], row['Trader'], row['Symbol'])
                cursor.execute("""
                    INSERT INTO cash_equity.policy_breaches_1 (
                        id, Trader, Datetime, Symbol, BuyNotional, SellNotional,
                        longlimit, shortlimit, longbreaches, shortbreaches
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_value, row['Trader'], row['DateTime'], row['Symbol'],
                    row['BuyNotional'], row['SellNotional'], row['longlimit'],
                    row['shortlimit'], row['longbreaches'], row['shortbreaches']
                ))
                
            # Insert data into policy_breaches_2
            for index, row in policy_breaches_2.iterrows():
                id_value = generate_id(row['DateTime'], row['Trader'], row['Symbol'])
                cursor.execute("""
                    INSERT INTO cash_equity.policy_breaches_2 (
                        id, Trader, Datetime, Symbol, BuyQuantity, SellQuantity,
                        totalVolume, TotalTrading, VR, volumelimit, volumebreaches
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_value, row['Trader'], row['DateTime'], row['Symbol'],
                    row['BuyQuantity'], row['SellQuantity'], row['totalVolume'],
                    row['TotalTrading'], row['VR'], row['volumelimit'], row['volumebreaches']
                ))

            connection.commit()
            print("Data loaded into PostgreSQL tables successfully.")
        except psycopg2.Error as e:
            connection.rollback()
            print("Error loading data into PostgreSQL tables:", e)
        finally:
            if cursor:
                cursor.close()
            connection.close()
    else:
        print("Connection to PostgreSQL failed.")

if __name__ == "__main__":
    main()



