import pandas as pd
import requests
import time
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
INPUT_FILE = '/Users/eshwar/Downloads/2026/Feb_2026/577245/PPRR-Extract/Book2.xlsx'  # Your source file
OUTPUT_FILE = '/Users/eshwar/Downloads/2026/Feb_2026/577245/PPRR-Extract/Reports/employee_report_2.xlsx'
COLUMN_NAME = 'mobile'
MAX_THREADS = 10
SLEEP_INTERVAL = 500
SLEEP_TIME = 60

# Fetch token from environment variable
BEARER_TOKEN = os.getenv('ACCESS_TOKEN') 

def fetch_employee_data(mobile):
    """Fetches employeeId and uuid for a given mobile number."""
    # Clean the mobile number to ensure it has the + prefix for the URL
    search_key = str(mobile).strip()
    if not search_key.startswith('+'):
        search_key = f"+{search_key}"
        
    url = f"https://cwms.ril.com/api/employee-mgmt/org/d0078c11-5a66-4e47-8a62-1eb4e3843282/employee/search"
    params = {
        'key': search_key,
        'category': 'mobile',
        'v': int(time.time() * 1000)
    }
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'authorization': f'Bearer {BEARER_TOKEN}',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                emp = data[0]
                return {
                    'mobile': mobile,
                    'employeeId': emp.get('employeeId', 'N/A'),
                    'uuid': emp.get('uuid', 'N/A')
                }
        return {'mobile': mobile, 'employeeId': 'Not Found', 'uuid': 'Not Found'}
    except Exception as e:
        return {'mobile': mobile, 'employeeId': f'Error: {str(e)}', 'uuid': 'Error'}

def main():
    if not BEARER_TOKEN:
        print("Error: Please set the CWMS_TOKEN environment variable.")
        return

    # Load Excel
    df = pd.read_excel(INPUT_FILE)
    numbers = df[COLUMN_NAME].tolist()
    results = []

    print(f"Starting processing for {len(numbers)} records...")

    # Process in chunks to handle the sleep requirement
    for i in range(0, len(numbers), SLEEP_INTERVAL):
        chunk = numbers[i : i + SLEEP_INTERVAL]
        
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_num = {executor.submit(fetch_employee_data, num): num for num in chunk}
            
            for future in tqdm(as_completed(future_to_num), total=len(chunk), desc=f"Batch {i//SLEEP_INTERVAL + 1}"):
                results.append(future.result())

        # Sleep logic
        if i + SLEEP_INTERVAL < len(numbers):
            print(f"Reached {i + SLEEP_INTERVAL} records. Sleeping for {SLEEP_TIME} seconds...")
            time.sleep(SLEEP_TIME)

    # Save to Excel
    output_df = pd.DataFrame(results)
    output_df.to_excel(OUTPUT_FILE, index=False)
    print(f"Done! Report saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()