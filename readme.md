# finance-database

A tool to collect stock price history from the internet, and incrementally store this into a local databse.

This might be useful if you want to monitor the performance of your investments.

Inputs (to run the script)
* companies-held.csv		A file describing the current shares in your portfolio
* finance-database.db		Optional, an SQLlite database onto which to append the data. Created if it doesn't exist.
   
Returns:
* finance-database.db		An updated database.
* output-current-price.csv	The current prices in csv format. Exported from the database.
* output-price-history.csv	The price history in csv format. Exported from the database.
