from yahoo_fin import options as op
import pandas as pd
from datetime import datetime, date
import numpy as np
import sqlite3 
import itertools
import time

#Variable to hold ticker
ticker = 'SPY'

#Variables to hold time, date, and expiration date data
today = date.today()
date_time = datetime.now()
file_date_time = date_time.strftime("%Y-%m-%d-%H-%M")
todays_date = datetime(today.year, today.month, today.day)
option_exp_dates = op.get_expiration_dates(ticker)

#Variable set to true for while loop
running = True

#While loop for constant iteration
while running == True:
    #For loop to iterate through each expiration date and find option chain
    for i in range(0, len(option_exp_dates)):

        option_exp_date = datetime.strptime(option_exp_dates[i], '%B %d, %Y')
        DTE = (option_exp_date - todays_date).days
        database_DOE = date_time.strftime("%Y-%m-%d %H:%M:%S")

        chain = op.get_options_chain(ticker, date= option_exp_dates[i])

        #Spliced index elements for main DataFrame
        call_strike = chain['calls']['Strike']
        call_last_price = chain['calls']['Last Price']
        call_bid = chain['calls']['Bid']
        call_ask = chain['calls']['Ask']
        call_change = chain['calls']['Change']
        call_change_percent = chain['calls']['% Change']
        call_volume = chain['calls']['Volume']
        call_open_interest = chain['calls']['Open Interest']
        call_implied_volatility = chain['calls']['Implied Volatility']

        put_strike = chain['puts']['Strike']
        put_last_price = chain['puts']['Last Price']
        put_bid = chain['puts']['Bid']
        put_ask = chain['puts']['Ask']
        put_change = chain['puts']['Change']
        put_change_percent = chain['puts']['% Change']
        put_volume = chain['puts']['Volume']
        put_open_interest = chain['puts']['Open Interest']
        put_implied_volatility = chain['puts']['Implied Volatility']

        #Main DataFrame
        df_main_option_chain = pd.DataFrame(data = list(zip(itertools.repeat(database_DOE), \
        call_strike, call_last_price, \
        call_bid, call_ask, call_change, call_change_percent, call_volume, \
        call_open_interest, call_implied_volatility, \
        put_strike, put_last_price, put_bid, put_ask, put_change, put_change_percent,\
        put_volume, put_open_interest, put_implied_volatility)), \
        columns= ['Date Of Entry', \
        'Call Strike', 'Call Last Price', 'Call Bid', 'Call Ask', 'Call Change', \
        'Call Change %', 'Call Volume', 'Call Open Interest', 'Call Implied Volatility',\
        'Put Strike', 'Put Last Price', 'Put Bid', 'Put Ask', 'Put Change', \
        'Put Change %', 'Put Volume', 'Put Open Interest', 'Put Implied Volatility'])

        #Create connection to database
        conn = sqlite3.connect('tutorial_data.db')

        #Export DataFrame to SQLite Database
        df_main_option_chain.to_sql('SPY_Option_Chain', conn, if_exists= 'append', \
        index=False)
        
        #Runs code every 5 minutes
        time.sleep(300)


