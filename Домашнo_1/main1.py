import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as BS
from concurrent.futures import ThreadPoolExecutor
import time

BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/{}"


def fetch_issuer_list():
    """
    Retrieve a list of issuers (company codes) available on the Macedonian Stock Exchange.
    Filters out options that contain numbers.
    """
    response = requests.get(BASE_URL.format("ADIN"))
    soup = BS(response.content, 'html.parser')
    option_elements = soup.select("#Code > option")
    issuers = [opt.text.strip() for opt in option_elements if not any(char.isdigit() for char in opt.text)]
    return issuers


def get_last_recorded_date(issuer_code):
    """
    Retrieve the most recent date of recorded data for a given issuer from the existing CSV file.
    If no file is found, return None to indicate no prior data exists.
    """
    try:
        df = pd.read_csv(f"data/{issuer_code}.csv")
        if df.empty:
            return None
        return pd.to_datetime(df['Date'].max())
    except FileNotFoundError:
        return None


def parse_row(row):
    """
    Extracts and organizes data from a table row element (HTML <tr>) to a structured dictionary.
    """
    cells = row.select("td")
    return {
        "Date": cells[0].text.strip(),
        "Year": cells[0].text.strip().split(".")[2],
        "Month": cells[0].text.strip().split(".")[1],
        "Price for Last Transaction": cells[1].text.strip(),
        "Max Price": cells[2].text.strip(),
        "Min Price": cells[3].text.strip(),
        "Average Price": cells[4].text.strip(),
        "% Change": cells[5].text.strip(),
        "Quantity": cells[6].text.strip(),
        "Market Volume (MKD)": cells[7].text.strip(),
        "Total Volume": cells[8].text.strip()
    }


def retrieve_page_data(session, url, payload):
    """
    Fetch data for a single page given a session, URL, and payload.
    Handles recursive pagination if a 'Next' button is available.
    """
    response = session.post(url, data=payload)
    if response.status_code != 200:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return []

    soup = BS(response.content, 'html.parser')
    rows = soup.select("#resultsTable > tbody > tr")
    records = [parse_row(row) for row in rows]

    next_button = soup.select_one(".next > a")
    if next_button:
        next_url = next_button.get("href")
        records.extend(retrieve_page_data(session, next_url, payload))

    return records


def gather_annual_data(issuer_code, year):
    payload = {
        'Code': issuer_code,
        'FromDate': f"01.01.{year}",
        'ToDate': f"31.12.{year}"
    }

    print(f"Collecting data for {issuer_code} in {year}...")

    with requests.Session() as session:
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        data = retrieve_page_data(session, BASE_URL.format(issuer_code), payload)

    return data[::-1]  


def update_issuer_data(issuer_code):
   
    try:
        last_date = get_last_recorded_date(issuer_code)
        start_date = datetime(2014, 11, 3) if not last_date else last_date + timedelta(days=1)
        current_date = datetime.now()

        all_data = []
        for year in range(start_date.year, current_date.year + 1):
            year_data = gather_annual_data(issuer_code, year)
            all_data.extend(year_data)

        df_new = pd.DataFrame(all_data)
        output_file = f"data/{issuer_code}.csv"
        if os.path.exists(output_file):
            df_existing = pd.read_csv(output_file)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        df_combined.to_csv(output_file, encoding="utf-8-sig", index=False)
        print(f"Data for {issuer_code} saved successfully.")

    except Exception as e:
        print(f"Failed to update data for {issuer_code}: {e}")


def main():
    issuer_codes = fetch_issuer_list()
    num_threads = 16  
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(update_issuer_data, issuer_codes)


if __name__ == '__main__':
    start_time = time.time()
    if not os.path.exists('data'):
        os.makedirs('data')
    main()
    elapsed_time = (time.time() - start_time) / 60
    print(f"Total runtime: {elapsed_time:.2f} minutes")
