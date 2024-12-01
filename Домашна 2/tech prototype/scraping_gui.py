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

BASE_URL = "https://www.mse.mk/mk/stats/symbolhistory/{}"

# --- Scraping Functions ---
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

def get_last_recorded_date(issuer_code):
    try:
        df = pd.read_csv(f"data/{issuer_code}.csv")
        if df.empty:
            return None
        return pd.to_datetime(df['Date'].max())
    except FileNotFoundError:
        return None

def parse_row(row):
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
    response = session.post(url, data=payload)
    if response.status_code != 200:
        log(f"Failed to retrieve data. Status code: {response.status_code}")
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

    log(f"Collecting data for {issuer_code} in {year}...")

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
        log(f"Data for {issuer_code} saved successfully.")

    except Exception as e:
        log(f"Failed to update data for {issuer_code}: {e}")

def main():
    issuer_codes = fetch_issuer_list()
    if not issuer_codes:
        log("No issuer codes found. Exiting.")
        return
    num_threads = 16
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(update_issuer_data, issuer_codes)

# --- GUI Code ---
def log(message):
    log_area.insert(tk.END, message + "\n")
    log_area.see(tk.END)

def start_scraping_thread():
    thread = threading.Thread(target=start_scraping)
    thread.start()

def start_scraping():
    log("Starting scraping...")
    start_time = time.time()
    if not os.path.exists('data'):
        os.makedirs('data')
    main()
    elapsed_time = (time.time() - start_time) / 60
    log(f"Scraping completed in {elapsed_time:.2f} minutes")
    messagebox.showinfo("Success", f"Scraping completed in {elapsed_time:.2f} minutes")

def login():
    username = username_entry.get()
    password = password_entry.get()

    if username == "admin" and password == "password":
        messagebox.showinfo("Login Successful", "Welcome!")
        login_window.destroy()
        show_scraping_interface()
    else:
        messagebox.showerror("Login Failed", "Invalid username or password!")

def show_scraping_interface():
    global log_area
    scraping_window = tk.Tk()
    scraping_window.title("Scraping Interface")

    tk.Label(scraping_window, text="Welcome to the Scraping Tool", font=("Arial", 16)).pack(pady=10)
    tk.Button(scraping_window, text="Start Scraping", font=("Arial", 14), command=start_scraping_thread).pack(pady=10)

    # Add the log area below the button
    log_area = scrolledtext.ScrolledText(scraping_window, wrap=tk.WORD, height=15, width=80, font=("Courier", 10))
    log_area.pack(pady=10)

    scraping_window.mainloop()

login_window = tk.Tk()
login_window.title("Login")
tk.Label(login_window, text="Username", font=("Arial", 14)).grid(row=0, column=0, padx=10, pady=10)
username_entry = tk.Entry(login_window, font=("Arial", 14))
username_entry.grid(row=0, column=1, padx=10, pady=10)
tk.Label(login_window, text="Password", font=("Arial", 14)).grid(row=1, column=0, padx=10, pady=10)
password_entry = tk.Entry(login_window, show="*", font=("Arial", 14))
password_entry.grid(row=1, column=1, padx=10, pady=10)
tk.Button(login_window, text="Login", font=("Arial", 14), command=login).grid(row=2, column=0, columnspan=2, pady=20)
login_window.mainloop()
