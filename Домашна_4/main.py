# Refactored code with the implementation of the Strategy pattern used in lectures

import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as BS
from concurrent.futures import ThreadPoolExecutor
import time
import matplotlib.pyplot as plt

# --- Constants ---
BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/{}"
DATA_FOLDER = "data"

# --- Helper Functions ---
def log_message(log_area, message):
    log_area.insert(tk.END, message + "\n")
    log_area.see(tk.END)

def ensure_folder_exists(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

# --- Data Fetching Strategies ---
class DataFetchStrategy:
    def fetch_data(self, *args, **kwargs):
        raise NotImplementedError("Fetch method must be implemented.")

class IssuerListStrategy(DataFetchStrategy):
    def fetch_data(self):
        try:
            response = requests.get(BASE_URL.format("ADIN"), headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code != 200:
                return []

            soup = BS(response.content, 'html.parser')
            option_elements = soup.select("#Code > option")

            return [opt.text.strip() for opt in option_elements if not any(char.isdigit() for char in opt.text)]
        except Exception as e:
            return []

class AnnualDataStrategy(DataFetchStrategy):
    def __init__(self, session):
        self.session = session

    def fetch_data(self, issuer_code, year):
        payload = {
            'Code': issuer_code,
            'FromDate': f"01.01.{year}",
            'ToDate': f"31.12.{year}"
        }
        response = self.session.post(BASE_URL.format(issuer_code), data=payload)
        if response.status_code != 200:
            return []

        soup = BS(response.content, 'html.parser')
        rows = soup.select("#resultsTable > tbody > tr")
        return [self.parse_row(row) for row in rows]

    def parse_row(self, row):
        cells = row.select("td")
        return {
            "Date": cells[0].text.strip(),
            "Price": cells[1].text.strip()
        }

# --- Main Data Manager ---
class DataManager:
    def __init__(self, strategy):
        self.strategy = strategy

    def set_strategy(self, strategy):
        self.strategy = strategy

    def execute(self, *args, **kwargs):
        return self.strategy.fetch_data(*args, **kwargs)

# --- GUI Components ---
def start_scraping_thread(log_area):
    thread = threading.Thread(target=start_scraping, args=(log_area,))
    thread.start()

def start_scraping(log_area):
    log_message(log_area, "Starting scraping...")
    start_time = time.time()
    ensure_folder_exists(DATA_FOLDER)

    # Fetch issuer list
    issuer_strategy = IssuerListStrategy()
    manager = DataManager(issuer_strategy)
    issuers = manager.execute()

    if not issuers:
        log_message(log_area, "No issuers found.")
        return

    # Fetch data for each issuer
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    annual_strategy = AnnualDataStrategy(session)
    manager.set_strategy(annual_strategy)

    for issuer in issuers:
        log_message(log_area, f"Fetching data for {issuer}...")
        for year in range(2014, datetime.now().year + 1):
            data = manager.execute(issuer, year)
            if data:
                save_data(issuer, data)

    elapsed_time = (time.time() - start_time) / 60
    log_message(log_area, f"Scraping completed in {elapsed_time:.2f} minutes.")

def save_data(issuer, data):
    file_path = os.path.join(DATA_FOLDER, f"{issuer}.csv")
    df = pd.DataFrame(data)
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        df = pd.concat([df_existing, df], ignore_index=True)
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

def create_gui():
    root = tk.Tk()
    root.title("Scraping Tool")

    tk.Label(root, text="Scraping Application", font=("Arial", 16)).pack(pady=10)

    log_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, height=15, width=80, font=("Courier", 10))
    log_area.pack(pady=10)

    tk.Button(root, text="Start Scraping", font=("Arial", 14), command=lambda: start_scraping_thread(log_area)).pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    create_gui()
