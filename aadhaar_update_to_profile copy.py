import os
import time
import uuid
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ================= CONFIG =================
BASE_URL = os.getenv("BASE_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

ORG_ID = "d0078c11-5a66-4e47-8a62-1eb4e3843282"
MAX_THREADS = 5
SLEEP_AFTER = 500
SLEEP_SECONDS = 60

HEADERS = {
    "accept": "application/json",
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "content-type": "application/json"
}

# =========================================

def search_employee(employee_id):
    url = f"{BASE_URL}/api/employee-mgmt/org/{ORG_ID}/employee/search"
    params = {"key": employee_id, "category": "profile"}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None


def get_employee_profile(emp_uuid):
    url = f"{BASE_URL}/api/employee-mgmt/org/{ORG_ID}/employee/{emp_uuid}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def update_employee_profile(emp_uuid, payload):
    url = f"{BASE_URL}/api/employee-mgmt/org/{ORG_ID}/employee/{emp_uuid}"
    r = requests.put(url, headers=HEADERS, json=payload, timeout=30)

    if r.status_code != 200:
        print("❌ PUT Failed:", r.status_code)
        print("Response:", r.text)

    r.raise_for_status()
    return True


def process_row(row):
    report = {
        "orgId": ORG_ID,
        "employeeId": row.employeeId,
        "uuid": None,
        "type": row.type,
        "documentNumber": row.documentNumber,
        "status": False
    }

    try:
        emp = search_employee(row.employeeId)
        if not emp:
            report["error"] = "Employee not found"
            return report

        emp_uuid = emp["uuid"]
        report["uuid"] = emp_uuid

        profile = get_employee_profile(emp_uuid)

        # ✅ FIX: Force document number to string
        doc_number = str(row.documentNumber).split(".")[0]

        docs = profile.get("documents", [])

        existing_doc = next(
            (d for d in docs if d.get("type") == row.type),
            None
        )

        if existing_doc:
            existing_doc["documentNumber"] = doc_number
        else:
            docs.append({
                "type": row.type,
                "documentNumber": doc_number,
                "name": f'{profile.get("firstName")} {profile.get("lastName")}',
                "dob": profile.get("dob"),
                "uuid": str(uuid.uuid4())
            })

        profile["documents"] = docs

        # ✅ Remove only system-managed fields
        profile.pop("_id", None)
        profile.pop("systemGeneratedAudit", None)

        profile["requestFrom"] = "UI"

        update_employee_profile(emp_uuid, profile)

        report["status"] = True

    except Exception as e:
        report["error"] = str(e)

    return report


def main(input_excel, output_report):
    # ✅ Optional improvement: read documentNumber as string directly
    df = pd.read_excel(input_excel, dtype={"documentNumber": str})

    results = []
    counter = 0

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [
            executor.submit(process_row, row)
            for row in df.itertuples(index=False)
        ]

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())
            counter += 1

            if counter % SLEEP_AFTER == 0:
                print(f"\n⏸ Sleeping {SLEEP_SECONDS}s after {counter} records")
                time.sleep(SLEEP_SECONDS)

    pd.DataFrame(results).to_excel(output_report, index=False)
    print("✅ Report generated:", output_report)


if __name__ == "__main__":
    main(
        input_excel="/Users/eshwar/Downloads/2026/Mar_2026/578015/Task_4/aadhaar_update.xlsx",
        output_report="/Users/eshwar/Downloads/2026/Mar_2026/578015/Task_4/Reports/document_update_report_All_profile.xlsx"
    )