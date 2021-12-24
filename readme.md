# financedatabase

A tool to collect stock price history from the internet, and incrementally store this locally

This might be useful if you want to monitor the performance of your invesments.

Inputs (to run the script)
* companies-held.csv		A file describing the current shares in your portfolion
* finance-database.db		Optional, an SQLlite database oneo which to append the data
   
Returns:
* finance-database.db		An updated database.
* output-current-price.csv	The current prices in csv format. Exported from the database.
* output-price-history.csv	The price history in csv format. Exported from the database.
