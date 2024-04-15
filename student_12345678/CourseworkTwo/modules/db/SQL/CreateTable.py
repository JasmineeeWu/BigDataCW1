import psycopg2
from psycopg2 import sql

# Function to connect to PostgreSQL
def connect():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            port="5439",
            database="fift"
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None

# Function to create the required tables
def create_tables(connection):
    if connection is not None:
        try:
            cursor = connection.cursor()

            # Create policy_breaches_1 table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cash_equity.policy_breaches_1 (
                    id TEXT PRIMARY KEY,
                    Trader TEXT NOT NULL,
                    Datetime TEXT NOT NULL,
                    Symbol TEXT NOT NULL,
                    BuyNotional NUMERIC,
                    SellNotional NUMERIC,
                    longlimit NUMERIC,
                    shortlimit NUMERIC,
                    longbreaches TEXT NOT NULL,
                    shortbreaches TEXT NOT NULL,
                    FOREIGN KEY (Symbol) REFERENCES cash_equity.equity_static(Symbol) ON DELETE CASCADE
                )
            """)

            # Create policy_breaches_2 table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cash_equity.policy_breaches_2 (
                    id TEXT PRIMARY KEY,
                    Trader TEXT NOT NULL,
                    Datetime TEXT NOT NULL,
                    Symbol TEXT NOT NULL,
                    BuyQuantity NUMERIC,
                    SellQuantity NUMERIC,
                    totalVolume NUMERIC,
                    TotalTrading NUMERIC,
                    VR NUMERIC,
                    volumelimit NUMERIC,
                    volumebreaches TEXT NOT NULL,
                    FOREIGN KEY (Symbol) REFERENCES cash_equity.equity_static(Symbol) ON DELETE CASCADE
                )
            """)

            connection.commit()
            print("Tables created successfully.")
        except psycopg2.Error as e:
            connection.rollback()
            print("Error creating tables:", e)
        finally:
            cursor.close()
    else:
        print("Connection to PostgreSQL failed.")

# Function to generate unique id based on datetime, trader, and symbol, function will be further used in Main.py for loading the retults
def generate_id(datetime, trader, symbol):
    return f"{datetime}_{trader}_{symbol}"

# Main function
def main():
    connection = connect()
    if connection:
        create_tables(connection)
        connection.close()

if __name__ == "__main__":
    main()

