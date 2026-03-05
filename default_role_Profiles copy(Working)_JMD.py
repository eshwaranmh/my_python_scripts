import requests
import time
import math
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ================= CONFIG =================
ORG_ID = "43d32946-741d-4987-a4c5-b04b8776f63e"
TOKEN = ""
BASE_URL = "https://cwms.ril.com/api"

MAX_RETRIES = 4
RETRY_SLEEP = 5
PAGE_SIZE = 30              # Reduced for stability
MAX_WORKERS = 2             # Reduced for server safety

SLEEP_AFTER = 1000
SLEEP_TIME = 120

EXCEL_FILE = "/Users/eshwar/Downloads/2026/Feb_2026/576229/DMD_Project/Task_2/Task_2_Fetching_Profiles_Existing_roles.xlsx"
EXCEL_COLUMN = "defaultRole"

OUTPUT_FILE = "/Users/eshwar/Downloads/2026/Feb_2026/576229/DMD_Project/Task_2/Reports/Profiles_With_Existing_defaultRole_task_2.csv"

# =========================================

# =========================================

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {TOKEN}"
}

PAYLOAD = {
    "sieve": {},
    "verify": {"status": [], "result": [], "missingInfo": [], "insufficientInfo": []},
    "attend": {"faceReg": [], "missingInfo": []},
    "onboard": {
        "epfRegStatus": [],
        "esicRegStatus": [],
        "subcode": [],
        "suspensionStatus": []
    },
    "payroll": {"missingInfo": []},
    "termsSieve": {}
}

# ---------- Persistent Session ----------
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=MAX_WORKERS,
    pool_maxsize=MAX_WORKERS
)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ---------- Load ROLE UUIDs ----------
def load_default_roles():
    df = pd.read_excel(EXCEL_FILE)
    role_uuids = df[EXCEL_COLUMN].dropna().astype(str).unique().tolist()
    print(f"✅ Loaded {len(role_uuids)} defaultRole UUIDs from Excel")
    return role_uuids


# ---------- Safe Request ----------
def safe_request(method, url, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(
                method,
                url,
                timeout=(10, 120),  # connect timeout, read timeout
                **kwargs
            )
            r.raise_for_status()
            return r

        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError
        ):
            print(f"⚠️ Timeout/Connection error (Attempt {attempt}/{MAX_RETRIES})")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(RETRY_SLEEP * attempt * 2)

        except requests.exceptions.HTTPError as e:
            if r.status_code >= 500:
                print(f"⚠️ Server error {r.status_code} (Attempt {attempt})")
                time.sleep(RETRY_SLEEP * attempt)
            else:
                raise

    raise Exception("Request failed after retries")


# ---------- Get count ----------
def get_count(role_uuid):
    try:
        url = f"{BASE_URL}/employee-mgmt/org/{ORG_ID}/employees"
        params = {
            "function": role_uuid,
            "isActive": "true",
            "isCount": "true"
        }
        r = safe_request("POST", url, headers=HEADERS, params=params, json=PAYLOAD)
        return r.json().get("count", 0)
    except Exception as e:
        print(f"❌ Count failed for role {role_uuid}: {e}")
        return 0


# ---------- Fetch page ----------
def fetch_page(role_uuid, page):
    try:
        url = f"{BASE_URL}/employee-mgmt/org/{ORG_ID}/employees"
        params = {
            "function": role_uuid,
            "isActive": "true",
            "allDetails": "true",
            "pageSize": PAGE_SIZE,
            "pageNumber": page
        }
        r = safe_request("POST", url, headers=HEADERS, params=params, json=PAYLOAD)
        return r.json()
    except Exception as e:
        print(f"❌ Failed role {role_uuid} page {page}: {e}")
        return []


# ---------- Main ----------
def main():
    role_uuids = load_default_roles()
    processed = 0

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "orgId",
                "employeeId",
                "uuid",
                "defaultRole",
                "defaultRole_uuid",
                "defaultLocation",
                "defaultLocation_uuid"
            ]
        )
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            for role_uuid in role_uuids:

                count = get_count(role_uuid)
                if count == 0:
                    continue

                pages = math.ceil(count / PAGE_SIZE)
                print(f"\n🔍 Role UUID {role_uuid} → {count} profiles ({pages} pages)")

                futures = [
                    executor.submit(fetch_page, role_uuid, page)
                    for page in range(1, pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"Role {role_uuid[:8]}",
                    unit="page"
                ):

                    try:
                        employees = future.result()
                    except Exception as e:
                        print(f"❌ Future failed: {e}")
                        continue

                    for emp in employees:
                        writer.writerow({
                            "orgId": ORG_ID,
                            "employeeId": emp.get("employeeId"),
                            "uuid": emp.get("uuid"),
                            "defaultRole": emp.get("defaultRole"),
                            "defaultRole_uuid": role_uuid,
                            "defaultLocation": emp.get("defaultLocation"),
                            "defaultLocation_uuid": None
                        })

                        processed += 1

                        if processed % SLEEP_AFTER == 0:
                            print(f"\n⏸ Sleeping {SLEEP_TIME}s after {processed} profiles")
                            time.sleep(SLEEP_TIME)

    print("\n✅ COMPLETED SUCCESSFULLY")
    print(f"🎯 Total profiles fetched: {processed}")
    print(f"📄 Output file: {OUTPUT_FILE}")


# ---------- Run ----------
if __name__ == "__main__":
    main()