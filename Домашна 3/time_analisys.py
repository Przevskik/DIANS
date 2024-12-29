import threading
import tkinter as tk
from tkinter import scrolledtext, messagebox
import pandas as pd
import requests
import os
from bs4 import BeautifulSoup as BS
from datetime import datetime, timedelta
import time  # Import time for performance analysis

BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/{}"
DATA_DIR = "data"  # Folder where the CSV files are saved

def fetch_issuer_list():
    try:
        response = requests.get(BASE_URL.format("ADIN"), headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            log(f"Failed to fetch issuer list. HTTP Status: {response.status_code}")
            return []

        soup = BS(response.content, 'html.parser')
        option_elements = soup.select("#Code > option")

        if not option_elements:
            log("No issuer options found on the page.")
            return []

        issuers = [opt.text.strip() for opt in option_elements if not any(char.isdigit() for char in opt.text)]
        log(f"Fetched issuers: {issuers}")
        return issuers

    except Exception as e:
        log(f"Error fetching issuer list: {e}")
        return []

def fetch_stock_data(issuer_code):
    try:
        payload = {
            'Code': issuer_code,
            'FromDate': '01.01.2014',
            'ToDate': '31.12.2024'
        }
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        response = session.post(BASE_URL.format(issuer_code), data=payload)
        
        if response.status_code != 200:
            log(f"Failed to retrieve data for {issuer_code}. Status code: {response.status_code}")
            return None

        soup = BS(response.content, 'html.parser')
        rows = soup.select("#resultsTable > tbody > tr")
        
        data = []
        for row in rows:
            cells = row.select("td")
            record = {
                "Date": cells[0].text.strip(),
                "Price for Last Transaction": float(cells[1].text.strip().replace(",", "")),
                "Max Price": float(cells[2].text.strip().replace(",", "")),
                "Min Price": float(cells[3].text.strip().replace(",", "")),
                "Average Price": float(cells[4].text.strip().replace(",", "")),
                "% Change": cells[5].text.strip(),
                "Quantity": int(cells[6].text.strip().replace(",", "")),
                "Market Volume (MKD)": cells[7].text.strip(),
                "Total Volume": int(cells[8].text.strip().replace(",", ""))
            }
            data.append(record)
        
        return pd.DataFrame(data)

    except Exception as e:
        log(f"Error fetching stock data for {issuer_code}: {e}")
        return None

def calculate_rsi(data, period=14):
    delta = data['Price for Last Transaction'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def generate_signals(data, period=14):
    signals = []
    for index, row in data.iterrows():
        rsi_signal = 'Buy' if row[f'RSI_{period}'] < 30 else ('Sell' if row[f'RSI_{period}'] > 70 else 'Hold')
        signals.append(rsi_signal)
    data[f'Signal_{period}'] = signals
    return data

def log(message):
    log_area.insert(tk.END, message + "\n")
    log_area.see(tk.END)

def start_scraping_thread():
    thread = threading.Thread(target=start_scraping)
    thread.start()

def start_scraping():
    # Start of time analysis
    start_time = time.time()  # Record the start time
    log("Starting scraping and analysis...")

    issuer_codes = fetch_issuer_list()
    if not issuer_codes:
        log("No issuer codes found. Exiting.")
        return

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for issuer in issuer_codes:
        log(f"Checking data for {issuer}...")
        file_path = f"{DATA_DIR}/{issuer}.csv"

        if os.path.exists(file_path):
            log(f"Data for {issuer} already exists. Skipping scraping.")
            data = pd.read_csv(file_path)
        else:
            log(f"Data for {issuer} not found. Starting scraping...")
            data = fetch_stock_data(issuer)
            if data is None:
                log(f"Failed to scrape data for {issuer}.")
                continue
            data.to_csv(file_path, index=False)
            log(f"Data for {issuer} scraped and saved.")

        data['RSI_14'] = calculate_rsi(data)
        data = generate_signals(data)

        data.to_csv(f"{DATA_DIR}/analysis_{issuer}.csv", index=False)
        log(f"Analysis for {issuer} completed and saved.")
    
    # End of time analysis
    elapsed_time = (time.time() - start_time) / 60  # Calculate elapsed time in minutes
    log(f"Scraping and analysis completed in {elapsed_time:.2f} minutes.")
    messagebox.showinfo("Success", f"Scraping and analysis completed in {elapsed_time:.2f} minutes!")

def show_scraping_interface():
    global log_area
    scraping_window = tk.Tk()
    scraping_window.title("Scraping Tool")

    log_area = scrolledtext.ScrolledText(scraping_window, wrap=tk.WORD, height=15, width=80, font=("Courier", 10))
    log_area.pack(pady=10)

    tk.Button(scraping_window, text="Start Scraping and Analyze", font=("Arial", 14), command=start_scraping_thread).pack(pady=10)

    scraping_window.mainloop()

show_scraping_interface()
