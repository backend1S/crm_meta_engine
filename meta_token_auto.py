import requests
import os
import time


ENV_FILE = ".env"


# ------------------------------------------------
# LOAD ENV
# ------------------------------------------------
def load_env():

    env = {}

    with open(ENV_FILE) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                env[k] = v

    return env


# ------------------------------------------------
# UPDATE ENV TOKEN
# ------------------------------------------------
def update_env(token):

    env = load_env()

    env["PAGE_ACCESS_TOKEN"] = token

    with open(ENV_FILE, "w") as f:
        for k, v in env.items():
            f.write(f"{k}={v}\n")

    print("✅ ENV token updated")


# ------------------------------------------------
# GENERATE LONG USER TOKEN
# ------------------------------------------------
def generate_long_token():

    env = load_env()

    APP_ID = env["APP_ID"]
    APP_SECRET = env["APP_SECRET"]
    SHORT_TOKEN = env["SHORT_TOKEN"]

    url = "https://graph.facebook.com/v25.0/oauth/access_token"

    params = {
        "grant_type": "fb_exchange_token",
        "client_id": APP_ID,
        "client_secret": APP_SECRET,
        "fb_exchange_token": SHORT_TOKEN
    }

    r = requests.get(url, params=params)

    data = r.json()

    if "access_token" in data:

        print("✅ Long user token generated")

        return data["access_token"]

    print("❌ Token generation failed", data)

    return None


# ------------------------------------------------
# GET PAGE ACCESS TOKEN
# ------------------------------------------------
def get_page_token(long_token):

    env = load_env()

    PAGE_ID = env["PAGE_ID"]

    url = "https://graph.facebook.com/v25.0/me/accounts"

    params = {
        "access_token": long_token
    }

    r = requests.get(url, params=params)

    data = r.json()

    if "data" not in data:
        print("❌ Failed getting pages", data)
        return None

    for page in data["data"]:

        if page["id"] == PAGE_ID:

            print("✅ Page token generated")

            return page["access_token"]

    return None


# ------------------------------------------------
# CHECK FORMS
# ------------------------------------------------
def check_forms(token):

    env = load_env()

    PAGE_ID = env["PAGE_ID"]

    url = f"https://graph.facebook.com/v25.0/{PAGE_ID}/leadgen_forms"

    params = {
        "access_token": token
    }

    r = requests.get(url, params=params)

    data = r.json()

    if "data" in data:
        return len(data["data"])

    return 0


# ------------------------------------------------
# AUTO TOKEN MANAGER
# ------------------------------------------------
def start_token_automation():

    print("🚀 Meta Token Automation Started")

    while True:

        try:

            env = load_env()

            token = env["PAGE_ACCESS_TOKEN"]

            forms = check_forms(token)

            print("📊 Forms count:", forms)

            if forms == 0:

                print("⚠ Forms = 0 → regenerating token")

                long_token = generate_long_token()

                if not long_token:
                    time.sleep(60)
                    continue

                page_token = get_page_token(long_token)

                if not page_token:
                    time.sleep(60)
                    continue

                update_env(page_token)

                print("🔥 New token applied automatically")

        except Exception as e:

            print("❌ Token automation error:", e)

        time.sleep(300)
