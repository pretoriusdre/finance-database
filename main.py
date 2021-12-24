# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 20:22:22 2021

@author: https://github.com/pretoriusdre/financedatabase
"""

import yfinance as yf
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta
import pandas as pd


def download_data(companies_held, engine):
    # Expects companies_held to be series-like object containing strings
    # For example companies_held = ['VGS.AX', 'VAS.AX']
    
    # All historical data which is retrieved:
    price_history = pd.DataFrame()
    current_price_only = pd.DataFrame()

    codes_downloaded = 0

    for ticker_code in companies_held:

        print('Starting on ' + ticker_code)
        
        # Get the starting date to retrieve from (last data)
        try:
            last_recorded_date = get_last_recorded_date(engine, ticker_code)
            next_date_to_dl = last_recorded_date + timedelta(days=1)
            next_date_to_dl = next_date_to_dl.strftime('%Y-%m-%d')
            print('    - Database was read ok. Next date to download is  '
                  + next_date_to_dl)
        except Exception as e:
            print(e)
            print('    - Local database could not be read. '
                  + 'Will try get all data')
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
                price_history = price_history.append(price_history_temp)
                current_price_only = current_price_only.append(
                    price_history.tail(1))
                codes_downloaded += 1
        except Exception as e:
            print(e)
            print('    - There was an error getting any price data')   

        time.sleep(0.5)  # to not overload the API

    print('Total of ' + str(len(price_history))
          + ' history days retrieved across '
          + str(codes_downloaded) + ' codes')
    
    return price_history, current_price_only


def get_last_recorded_date(engine, ticker_code):
    last_recorded_date = engine.execute(
        "SELECT Date FROM price_history where ticker_code = '"
        + ticker_code
        + "'  order by date desc"
    ).fetchone()[0]
    last_recorded_date = datetime.strptime(
        last_recorded_date[:10], '%Y-%m-%d')
    return last_recorded_date


def commit_data(data, table, engine, mode):
    data.to_sql(table, con=engine, if_exists=mode)
    print('    - ' + str(len(data))
          + ' records saved to table ' + table)


def export_data_to_csv(output_file, table, engine):
    # These output copies of the table to csv files, for use in PowerBI
    temp_df = pd.read_sql(table, con=engine)
    temp_df.to_csv(output_file)


def get_duplicate_records(engine, table):
    try:
        duplicates = engine.execute("SELECT ticker_code, Date, COUNT(*) FROM "
                                    + table
                                    + " GROUP BY ticker_code,"
                                    + " Date HAVING COUNT(*) > 1").fetchall()
        return duplicates
    except Exception as e:
        print(e)
        print('    - Local database could not be read. ')
    return None


def delete_duplicate_records(engine, table):
    engine.execute("DELETE FROM " + table
                   + " WHERE rowid NOT IN ("
                   + " SELECT MIN(rowid) "
                   + " FROM " + table
                   + " GROUP BY "
                   + " ticker_code, Date)"
                   )


def manual_add_missing_data(data, table, engine):
    data.to_sql(table, con=engine, if_exists='append')


def main():
    # Local sqlalchemy database
    file_finance_database = 'finance-database.db'

    # Must have a column called 'Code'
    # Code must contain strings with a ticker matching Yahoo Finance, such as 'VGS.AX'
    file_companies_held = 'companies-held.csv'

    # Files for manually loading in missing data, if applicable
    file_missing_data_delisted = 'input-missing-data-delisted.csv'

    # Data output for PowerBI. Historical timeseries and current price.
    file_price_history_output = 'output-price-history.csv'
    file_current_price_output = 'output-current-price.csv'

    engine = create_engine('sqlite:///' + file_finance_database, echo=False)

    companies_held = pd.read_csv(file_companies_held)

    res = input('Type Y to download all company data\n')
    if res.upper() == 'Y':
        (price_history, current_price_only) = download_data(companies_held['Code'], engine)
        res = input('Type Y to commit the data to the local database\n')
        if res.upper() == 'Y':
            commit_data(price_history, 'price_history', engine, 'append')
            commit_data(current_price_only, 'current_price_only', engine, 'replace')

    res = ''
    # Add in missing data, eg for delisted symbols. Only needed once. I don't need this anymore
    # res = input('Type Q to upload the manual data into the database\n')
    if res.upper() == 'Q':
        try:
            missing_data = pd.read_csv(file_missing_data_delisted)
            missing_data = missing_data.set_index('Date')
            manual_add_missing_data(missing_data, 'price_history', engine)
        except:
            print('Something went wrong trying to load in the manual data.')

    print('duplicates are:')
    duplicates = get_duplicate_records(engine, 'price_history')
    print(duplicates)

    res = input('Type Y to delete duplicate records\n')
    if res.upper() == 'Y':
        delete_duplicate_records(engine, 'price_history')

    print('duplicates are:')
    duplicates = get_duplicate_records(engine, 'price_history')
    print(duplicates)

    res = input('Type Y to export CSV files for PowerBI\n')
    if res.upper() == 'Y':
        # These output copies of the database to csv files, for use in PowerBI
        export_data_to_csv(output_file=file_price_history_output,
                           table='price_history',
                           engine=engine)

        export_data_to_csv(output_file=file_current_price_output,
                           table='current_price_only',
                           engine=engine)


if __name__ == '__main__':
    main()
