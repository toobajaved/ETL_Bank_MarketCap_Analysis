# ETL Bank Market Cap Analysis

## Description:
This project demonstrates an ETL (Extract, Transform, Load) pipeline developed in Python, focusing on extracting bank market capitalization data from Wikipedia, transforming it using exchange rates, and loading the processed data into both a CSV file and an SQLite database.

## Table of Contents
- Project Overview
- Prerequisites
- Installation
- Usage
- Code Explanation
- License

## Project Overview

The ETL pipeline automates the process of extracting data on the world's largest banks by market cap from a Wikipedia page, converting the data into multiple currencies using exchange rates, and loading the cleaned data into both a CSV file and an SQLite database for further analysis.
Prerequisites

## Prerequisites
Ensure you have the following installed on your local machine:

**Python 3.7 or above**

**Python Libraries:**

  - requests
- beautifulsoup4
- pandas
- numpy
- sqlite3
        
To install the required libraries, you can use the command:

    pip install requests beautifulsoup4 pandas numpy 

## Installation

Clone the repository:

    git clone https://github.com/YourUsername/ETL_Bank_Market_cap_analysis.git

 Navigate to the project directory:

    cd ETL_Bank_Market_cap_analysis

## Usage

   Make sure the exchange_rate.csv file is available in the project directory.
   Run the main ETL script:

    python etl_bankscript.py

   Check the outputs:
  The transformed data will be saved as Largest_banks_data.csv.
        The data is also loaded into an SQLite database named Banks.db.

## Code Explanation

The script follows these main steps:

- Extract: The code scrapes the list of the largest banks by market capitalization from a Wikipedia page using BeautifulSoup.
- Transform: The market capitalization data in USD is converted into GBP, EUR, and INR using exchange rates from a CSV file.
- Load: The transformed data is saved into a CSV file and loaded into an SQLite database.

## License

This project is licensed under the [MIT License](LICENSE) - see the [LICENSE](LICENSE) file for details.


