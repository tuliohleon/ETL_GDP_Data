# ETL Pipeline for Extracting and Transforming GDP Data

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline to scrape GDP data from a web archive of a Wikipedia page. The data is extracted, cleaned, transformed, and saved into a database and a CSV file. It includes functionality to query the processed data based on specified conditions.

## Features
- **Extract:** Scrapes tabular GDP data from a given URL using BeautifulSoup.
- **Transform:** Converts GDP values from millions to billions of USD, rounding to two decimal places.
- **Load:** Stores the processed data in a SQLite database and exports it as a CSV file.
- **Query:** Allows SQL-based querying of the processed data.
- **Logging:** Logs progress at each stage of the ETL process.

## Prerequisites
### Libraries and Dependencies
Ensure the following Python libraries are installed:
- `BeautifulSoup4`
- `requests`
- `pandas`
- `numpy`
- `sqlite3` (standard library)

Install missing libraries using `pip`:
```bash
pip install beautifulsoup4 requests pandas numpy
```

### Required Tools
- Python 3.6 or higher.
- SQLite database (pre-installed with Python).

## Project Structure
- **`extract(url, table_attribs)`**
  Extracts data from the specified URL and returns a Pandas DataFrame.

- **`transform(df)`**
  Transforms GDP data from millions to billions and rounds values to two decimal places.

- **`load_to_csv(df, csv_path)`**
  Saves the DataFrame to a CSV file at the specified path.

- **`load_to_db(df, sql_connection, table_name)`**
  Saves the DataFrame as a table in a SQLite database.

- **`run_query(query_statement, sql_connection)`**
  Runs a SQL query on the database and prints the results.

- **`log_progress(message)`**
  Logs progress messages at each stage of the ETL process.

## Usage
### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. Define Constants
Edit the script to set the following constants:
```python
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
```

### 3. Run the Script
Execute the script to start the ETL process:
```bash
python etl_script.py
```

### 4. Query the Database
After execution, you can query the database for GDP data exceeding 100 billion USD:
```sql
SELECT * FROM Countries_by_GDP WHERE GDP_USD_billions >= 100;
```

## Output
1. **CSV File:**
   - Saved at the path specified by `csv_path`.
   - Contains transformed GDP data.

2. **SQLite Database Table:**
   - Database: `World_Economies.db`
   - Table: `Countries_by_GDP`

3. **Query Results:**
   - Printed on the terminal for the specified condition.

## Sample Log Messages
- `Preliminaries complete. Initiating ETL process.`
- `Call extract() function. Data extraction complete. Initiating Transformation process.`
- `Data transformation complete. Initiating loading process.`
- `Data saved to CSV file.`
- `SQL Connection initiated.`
- `Data loaded to Database as table. Running the query.`
- `Process Complete.`

## License
This project is licensed under the MIT License.

## Author
[Tulio Hernán Leon Daza]

