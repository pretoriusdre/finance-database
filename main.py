# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 20:22:22 2021

@author: https://github.com/pretoriusdre/financedatabase
"""

import yfinance as yf
from sqlalchemy import create_engine
import string
import time
from datetime import date, datetime, timedelta
import pandas as pd
from sqlite_wrapper import SQLiteWrapper
import shutil
from pathlib import Path



class FinanceDatabaseWrapper(SQLiteWrapper):

    ddl_creation_statements = [
        """
        CREATE TABLE IF NOT EXISTS price_history  (
            [id]            TEXT PRIMARY KEY,
            [ticker_code]   TEXT,
            [date]          DATE,
            [open]          FLOAT,
            [high]          FLOAT,
            [low]           FLOAT,
            [close]         FLOAT,
            [volume]        BIGINT,
            [dividends]     BIGINT,
            [stock_splits]  BIGINT
        );"""
        ,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_price_history_ticker_code_date
        ON price_history ([ticker_code], [date]);
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS price_current  (
            [id]            TEXT PRIMARY KEY,
            [ticker_code]   TEXT,
            [date]          DATE,
            [open]          FLOAT,
            [high]          FLOAT,
            [low]           FLOAT,
            [close]         FLOAT,
            [volume]        BIGINT,
            [dividends]     BIGINT,
            [stock_splits]  BIGINT
        );
        """
        ,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_price_current_ticker_code_date
        ON price_current ([ticker_code]);
        """
    ]


    def __init__(self):


        super().__init__(db_path=Path('finance-database.db'), create=True)

        self.backup()

        for statement in FinanceDatabaseWrapper.ddl_creation_statements:

            self.execute(statement)


    def backup(self):
        
        today_date = date.today()
        last_backup_date = date.fromisoformat('2000-01-01')
        
        backup_path = Path('db_backups') 

        for path in backup_path.glob('*.db'):
            backup_date = date.fromisoformat(path.name[:10])
            last_backup_date = max(last_backup_date, backup_date)


        if (today_date - last_backup_date).days >= 7:
            backup_file_path = backup_path / f'{today_date.isoformat()}.db'
            shutil.copy(self.db_path, backup_file_path)
        

    def export_data_to_csv(self, table, output_file):
        # These output copies of the table to csv files, for use in PowerBI
        df = self.get_table(table)
        print(f'Exporting {len(df)} rows')
        df.to_csv(output_file, index=False)


    def get_last_recorded_date(self, ticker_code):

        # ticker_code = self._sanitise_input(ticker_code)
        query = f"""
        SELECT date FROM price_history
        WHERE ticker_code = ?
        ORDER BY date desc
        """
        records = self.execute(query,parameters=(ticker_code,), fetch=True)
        last_recorded_date = records[0][0] # First record, first column

        if type(last_recorded_date) is str:
            last_recorded_date = last_recorded_date[:10]
            return date.fromisoformat(last_recorded_date)
        elif type(last_recorded_date) is datetime:
            return last_recorded_date.date()
        elif type(last_recorded_date) is date:
            return last_recorded_date
    
        raise ValueError(f'Bad date encountered: {last_recorded_date}')


    def delete_last_n_days(self, n):
        delete_from = date.today() - timedelta(days=n)
        delete_from_str = delete_from.isoformat()
        delete_query = (
            f"""
            DELETE FROM price_history
            WHERE date >= '{delete_from_str}'
            """
        )
        self.execute(delete_query)



def download_data(companies_held, db):
    # Expects companies_held to be series-like object containing strings
    # For example companies_held = ['VGS.AX', 'VAS.AX']
    
    # All historical data which is retrieved:
    price_history_all = []
    price_current_all = []

    codes_downloaded = 0

    for ticker_code in companies_held:

        print('Starting on ' + ticker_code)
        
        # Get the starting date to retrieve from (last data)
        try:
            last_recorded_date = db.get_last_recorded_date(ticker_code)
            next_date_to_dl = last_recorded_date + timedelta(days=1)
            next_date_to_dl = next_date_to_dl.isoformat()
            print(f'    - Database was read ok. Next date to download is {next_date_to_dl}')
        except Exception as e:
            print(e)
            print('    - Local database could not be read. Will try get all data')
            # No data? Then start getting data from 2000
            # I don't need data before this    
            next_date_to_dl = '2000-01-01'
            
        try:            
            yfinance_obj = yf.Ticker(ticker_code)
            price_history_temp = yfinance_obj.history(start=next_date_to_dl)
            price_history_temp['ticker_code'] = ticker_code
            print('    - ' + str(len(price_history_temp))
                  + ' history days retrieved')
            if len(price_history_temp) > 0:
                price_history_all.append(price_history_temp)
                price_current_all.append(price_history_temp.tail(1))
                codes_downloaded += 1
        except Exception as e:
            print(e)
            print('    - There was an error getting any price data')   

        time.sleep(0.5)  # to not overload the API

    price_history = pd.concat(price_history_all)
    price_current = pd.concat(price_current_all)
    
    # Change the 'Date' dataframe index to columns
    price_history = price_history.reset_index()
    price_current = price_current.reset_index()

    price_history.columns = [to_snake_case(col) for col in price_history.columns]
    price_current.columns = [to_snake_case(col) for col in price_current.columns]
    
    print(f'Total of {len(price_history)} history days retrieved across {codes_downloaded} codes')
    
    # <class 'pandas._libs.tslibs.timestamps.Timestamp'> causing problems
    price_history['date'] = pd.to_datetime(price_history['date']).apply(lambda x: x.date().isoformat())
    price_current['date'] = pd.to_datetime(price_current['date']).apply(lambda x: x.date().isoformat())

    return price_history, price_current


def manual_add_missing_data(data, table, engine):
    data.to_sql(table, con=engine, if_exists='append')


def to_snake_case(text):
    allowable_chars = string.ascii_letters + string.digits
    snake_case = ''.join([char if char in allowable_chars else '_' for char in text]).lower()
    return snake_case

def main():

    db = FinanceDatabaseWrapper()


    # Must have a column called 'Code'
    # Code must contain strings with a ticker matching Yahoo Finance, such as 'VGS.AX'
    file_companies_held = 'companies-held.csv'

    # Files for manually loading in missing data, if applicable
    file_missing_data_delisted = 'input-missing-data-delisted.csv'

    # Data output for PowerBI. Historical timeseries and current price.
    output_file_price_history = 'price-history.csv'
    output_file_price_current = 'price-current.csv'

    
    companies_held = pd.read_csv(file_companies_held)

    
    # res = input('Type X to delete the last N days:\n')
    # if res.upper() == 'X':
    #     n = int(input('How many days?:\n'))
    #     delete_last_n_days(engine, n)




    (price_history, price_current) = download_data(companies_held['Code'], db)


    db.save_data(
        df=price_history,
        table_name='price_history',
        if_exists='upsert',
        unique_key=['date', 'ticker_code'],
        auto_add_id=True
        )
    db.save_data(
        df=price_current,
        table_name='price_current',
        if_exists='replace',
        auto_add_id=True
        )

    # These output copies of the database to csv files, for use in PowerBI.
    db.export_data_to_csv(
        table='price_history',
        output_file=output_file_price_history
        )
    db.export_data_to_csv(
        table='price_current',
        output_file=output_file_price_current
        )


if __name__ == '__main__':
    main()
