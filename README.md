### Internship, Glue Labs Private Limited.
#### Mentors: Vishal Chandra, Avinash Gupta

#### Basics of the Rust programming language:
1. How to use Cargo
2. Basic programming concepts (variables, data types, functions, control flow, etc)
3. Ownership rules in Rust
4. Implementation of structs, enums, match operator, vectors, strings, and hashmaps
5. Managing Rust projects using packages, crates, and modules
6. Error handling
7. Generic types and traits
8. Crates: csv, chrono, serde, xgboost, ta, and polars

#### Basics of stock trading:
1. Candlestick chart analysis
2. Processing open high close low data for a stock
3. Using technical analysis indicators like Simple moving averages, exponential moving averages, Fibonacci pivot points, Camarilla pivot points, etc

#### Machine Learning for stock trading:
1. Decision trees
2. Regression and classification
3. Gradient boosting: AdaBoost and XGBoost
4. Applying XGBoost on OHLC data to predict the close price

#### Processing the OHLC data using csv reader on Rust:
1. Loading and parsing data from the CSV file
2. Dividing the stock data based on instrument id
3. Creating 5-minute and 15-minute OHLC dataframes from 1-minute OHLC data
4. Finding indicators EMA20, EMA50, EMA200, and Fibonacci pivot levels on the given OHLC data

#### Applying the XGBoost algorithm on the given data using csv reader iteratively (inefficient method) on RUST:
1. Sorting the data by the “datetime” column using a vector of structs and storing the sorted data in a CSV file
2. Finding 5-minute and 1-day OHLC data from the sorted 1-minute OHLC data
3. Finding indicators EMA9 and EMA21 and storing them in a CSV file
4. Generating an I/O model by parsing data from the OHLC data and the indicators found and storing it in a CSV file
5. Dividing the final dataset into x_train, y_train, x_test, and y_test and converting the CSV input into xgboost’s data format i.e. f32 arrays using CSV reader
Applying the XGBoost algorithm using the xgboost crate to predict prices and calculate RMS prediction error by comparing them with the actual prices

#### Using polars dataframe library in Rust to implement an I/O model to increase efficiency:
1. Parsing the different columns of the OHLC using a vector schema
2. Sorting the OHLC dataframe by datetime 
3. Creating different dataframes by grouping by instrument id
4. Finding the 5-minute, 15-minute, and 1-day OHLC dataframes by downsampling the given 1 min OHLC data
5. Generating an I/O model by parsing data from the 1-day OHLC dataframe and storing it in a vector