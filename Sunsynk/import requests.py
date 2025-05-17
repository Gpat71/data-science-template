import requests
import pandas as pd
import schedule
import time

# API endpoints
AUTH_URL = "https://api.sunsynk.com/authenticate"
DATA_URL = "https://api.sunsynk.com/new-data-file"

# Authentication payload
AUTH_PAYLOAD = {
    "areaCode": "sunsynk",
    "client_id": "csp-web",
    "grant_type": "password",
    "password": "YourSunsynkPassword",
    "source": "sunsynk",
    "username": "YourSunsynkUsername",
}

HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


def get_access_token():
    """Authenticate and retrieve the access token."""
    try:
        response = requests.post(AUTH_URL, json=AUTH_PAYLOAD, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        if data["success"]:
            token = data["data"]["access_token"]
            print("Authentication successful.")
            return token
        else:
            print("Authentication failed:", data.get("msg", "Unknown error"))
            return None
    except requests.exceptions.RequestException as e:
        print(f"Authentication request failed: {e}")
        return None


def fetch_new_data():
    """Fetch the latest data file using the access token."""
    token = get_access_token()
    if not token:
        print("Skipping data retrieval due to authentication failure.")
        return None

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        response = requests.get(DATA_URL, headers=headers)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        print("New data file successfully retrieved and stored in DataFrame.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Data request failed: {e}")
        return None


# Schedule the task to run every hour
schedule.every().hour.do(fetch_new_data)

print("Script started. Fetching new data file every hour...")
while True:
    schedule.run_pending()
    time.sleep(60)
