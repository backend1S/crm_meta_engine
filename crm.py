import os
import json
import time
import threading
import requests
from datetime import datetime
from fastapi import APIRouter, Request
from dotenv import load_dotenv
from database import Database   # FIXED

load_dotenv()

router = APIRouter(prefix="/crm", tags=["CRM"])

PAGE_ID = os.getenv("PAGE_ID")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")


# ------------------------------------------------
# LOGGER
# ------------------------------------------------
def log(msg):
    print(msg)


# ------------------------------------------------
# FORMAT DATE
# ------------------------------------------------
def format_meta_date(meta_date):

    try:
        dt = datetime.strptime(meta_date, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


# ------------------------------------------------
# FETCH LEAD FROM META
# ------------------------------------------------
def fetch_lead(lead_id):

    try:

        url = f"https://graph.facebook.com/v25.0/{lead_id}"

        params = {
            "access_token": PAGE_ACCESS_TOKEN,
            "fields": ",".join([
                "id",
                "created_time",
                "ad_id",
                "ad_name",
                "adset_id",
                "adset_name",
                "campaign_id",
                "campaign_name",
                "form_id",
                "is_organic",
                "platform",
                "custom_disclaimer_responses",
                "field_data"
            ])
        }

        res = requests.get(url, params=params, timeout=10)

        data = res.json()

        if "error" in data:
            log(f"❌ Meta API Error → {data['error']}")
            return None

        return data

    except Exception as e:
        log(f"❌ Fetch Lead Error → {e}")
        return None


# ------------------------------------------------
# GET FORM NAME
# ------------------------------------------------
def get_form_name(form_id):

    try:

        url = f"https://graph.facebook.com/v25.0/{form_id}"

        params = {
            "access_token": PAGE_ACCESS_TOKEN,
            "fields": "name"
        }

        res = requests.get(url, params=params, timeout=10)

        return res.json().get("name", "")

    except Exception:
        return ""


# ------------------------------------------------
# EXTRACT FIELDS
# ------------------------------------------------
def extract_fields(meta_data):

    fields = {}

    for field in meta_data.get("field_data", []):

        name = field.get("name", "").replace("?", "").lower()

        values = field.get("values", [])

        value = values[0] if values else ""

        fields[name] = value

    return fields


# ------------------------------------------------
# DUPLICATE CHECK
# ------------------------------------------------
def lead_exists(lead_id):

    db = Database()

    sql = f"""
    SELECT TOP 1 ID
    FROM dbo.LEADS
    WHERE LEAD_ID = '{lead_id}'
    """

    status, rows = db.db_query(sql)

    if status and rows:
        return True

    return False


# ------------------------------------------------
# SAVE LEAD
# ------------------------------------------------
def save_lead(meta_data):

    try:

        if not meta_data:
            return

        db = Database()

        lead_id = meta_data.get("id")

        if not lead_id:
            return

        if lead_exists(lead_id):
            log(f"⚠ Duplicate skipped → {lead_id}")
            return

        fields = extract_fields(meta_data)

        created_time = format_meta_date(meta_data.get("created_time"))

        phone = fields.get("phone_number") or fields.get("whatsapp_number") or ""

        travel_date = (
            fields.get("preferred_travel_date")
            or fields.get("preferred_travel_month_or_date")
            or ""
        )

        form_id = meta_data.get("form_id")

        form_name = get_form_name(form_id)

        raw_json = json.dumps(meta_data).replace("'", "''")

        sql = f"""
        INSERT INTO dbo.LEADS (
            LEAD_ID,
            CREATED_TIME,
            AD_ID,
            AD_NAME,
            ADSET_ID,
            ADSET_NAME,
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            FORM_ID,
            FORM_NAME,
            IS_ORGANIC,
            PLATFORM,
            FULL_NAME,
            PHONE_NUMBER,
            EMAIL,
            CITY,
            PREFERRED_TRAVEL_DATE,
            PREFERRED_MODE_OF_CONTACT,
            HOW_MANY_PEOPLE_ARE_TRAVELLING_WITH,
            RAW_LEAD_JSON,
            CREATED_DATE,
            MODIFIED_DATE
        )
        VALUES (
            '{lead_id}',
            '{created_time}',
            '{meta_data.get("ad_id","")}',
            '{meta_data.get("ad_name","")}',
            '{meta_data.get("adset_id","")}',
            '{meta_data.get("adset_name","")}',
            '{meta_data.get("campaign_id","")}',
            '{meta_data.get("campaign_name","")}',
            '{form_id}',
            '{form_name}',
            '{meta_data.get("is_organic","")}',
            '{meta_data.get("platform","")}',
            '{fields.get("full_name","")}',
            '{phone}',
            '{fields.get("email","")}',
            '{fields.get("city","")}',
            '{travel_date}',
            '{fields.get("preferred_mode_of_contact","")}',
            '{fields.get("how_many_people_are_travelling_with","")}',
            '{raw_json}',
            GETDATE(),
            GETDATE()
        )
        """

        status, result = db.db_update(sql)

        if status:
            log(f"💾 Lead Saved → {lead_id}")
        else:
            log(f"❌ DB Error → {result}")

    except Exception as e:
        log(f"❌ Save Error → {e}")


# ------------------------------------------------
# PROCESS LEAD
# ------------------------------------------------
def process_lead(lead_id):

    try:

        if not lead_id:
            return

        meta_data = fetch_lead(lead_id)

        save_lead(meta_data)

    except Exception as e:
        log(f"❌ Lead Error → {e}")


# ------------------------------------------------
# GET ALL FORMS
# ------------------------------------------------
def get_all_forms():

    forms = []

    url = f"https://graph.facebook.com/v25.0/{PAGE_ID}/leadgen_forms"

    params = {
        "access_token": PAGE_ACCESS_TOKEN,
        "limit": 100
    }

    while url:

        try:

            res = requests.get(url, params=params, timeout=10)

            data = res.json()

            forms.extend(data.get("data", []))

            paging = data.get("paging", {})

            url = paging.get("next")

            params = None

        except Exception as e:

            log(f"Forms fetch error → {e}")
            break

    return forms


# ------------------------------------------------
# IMPORT ALL LEADS
# ------------------------------------------------
def import_all_leads():

    log("🚀 Importing ALL forms and leads")

    forms = get_all_forms()

    log(f"Total forms → {len(forms)}")

    for form in forms:

        form_id = form.get("id")

        log(f"Fetching leads from form → {form_id}")

        url = f"https://graph.facebook.com/v25.0/{form_id}/leads"

        params = {
            "access_token": PAGE_ACCESS_TOKEN,
            "limit": 100
        }

        while url:

            try:

                res = requests.get(url, params=params, timeout=10)

                data = res.json()

                leads = data.get("data", [])

                for lead in leads:

                    process_lead(lead.get("id"))

                paging = data.get("paging", {})

                url = paging.get("next")

                params = None

            except Exception as e:

                log(f"Lead import error → {e}")
                break

    log("✅ Import completed")


# ------------------------------------------------
# POLLING BACKUP
# ------------------------------------------------
def poll_leads():

    while True:

        try:

            log("🔄 Polling Meta backup leads")

            forms = get_all_forms()

            for form in forms:

                form_id = form.get("id")

                url = f"https://graph.facebook.com/v25.0/{form_id}/leads"

                params = {
                    "access_token": PAGE_ACCESS_TOKEN,
                    "limit": 10
                }

                res = requests.get(url, params=params, timeout=10)

                leads = res.json().get("data", [])

                for lead in leads:

                    process_lead(lead.get("id"))

        except Exception as e:

            log(f"Polling error → {e}")

        time.sleep(300)


# ------------------------------------------------
# START POLLING
# ------------------------------------------------
def start_polling():

    log("🚀 Facebook Lead Engine Started")

    thread = threading.Thread(target=poll_leads)

    thread.daemon = True

    thread.start()


# ------------------------------------------------
# WEBHOOK VERIFY
# ------------------------------------------------
@router.get("/webhook")
async def verify_webhook(request: Request):

    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return int(challenge)

    return {"status": "error"}


# ------------------------------------------------
# WEBHOOK RECEIVE
# ------------------------------------------------
@router.post("/webhook")
async def webhook(request: Request):

    body = await request.json()

    log("⚡ Webhook received")

    for entry in body.get("entry", []):

        for change in entry.get("changes", []):

            if change.get("field") != "leadgen":
                continue

            lead_id = change.get("value", {}).get("leadgen_id")

            if lead_id:

                thread = threading.Thread(
                    target=process_lead,
                    args=(lead_id,)
                )

                thread.daemon = True
                thread.start()

    return {"status": "ok"}