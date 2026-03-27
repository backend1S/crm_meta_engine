import os
import json
import time
import queue
import threading
import requests
from datetime import datetime
from fastapi import APIRouter, Request
from dotenv import load_dotenv
from database import Database

load_dotenv()

router = APIRouter(prefix="/crm", tags=["CRM"])

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "")

# ================================
# SETTINGS
# ================================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "40"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BUFFER_FLUSH_SECONDS = int(os.getenv("BUFFER_FLUSH_SECONDS", "10"))

# ================================
# RUNTIME
# ================================
PAGE_TOKENS = {}
FORM_NAME_CACHE = {}
LEAD_QUEUE = queue.Queue(maxsize=50000)
INSERT_BUFFER = []
BUFFER_LOCK = threading.Lock()
WORKERS_STARTED = False
IMPORT_DONE = False
LAST_PAGE_REFRESH = 0
FLUSHER_STARTED = False


# --------------------------------
# LOGGER
# --------------------------------
def log(msg):
    print(msg, flush=True)


# --------------------------------
# SAFE GET
# --------------------------------
def safe_get(url, params=None):
    for i in range(MAX_RETRIES):
        try:
            res = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if res.status_code == 200:
                return res

            log(f"⚠ HTTP {res.status_code} → {url}")

            try:
                log(f"⚠ Response → {res.text}")
            except:
                pass

        except Exception as e:
            log(f"⚠ Retry {i+1}/{MAX_RETRIES} → {e}")

        time.sleep(2 + i)

    return None


# --------------------------------
# DATE FORMAT
# --------------------------------
def format_date(meta_date):
    try:
        dt = datetime.strptime(meta_date, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


# --------------------------------
# EXTRACT FIELDS
# --------------------------------
def extract(meta):
    fields = {}

    for f in meta.get("field_data", []):
        name = f.get("name", "").replace("?", "").lower().strip()
        values = f.get("values", [])
        value = values[0] if values else ""
        fields[name] = value

    return fields


# --------------------------------
# ESCAPE SQL
# --------------------------------
def esc(v):
    return str(v).replace("'", "''") if v is not None else ""


# --------------------------------
# LOAD ALL PAGES
# --------------------------------
def load_pages(force=False):
    global PAGE_TOKENS, LAST_PAGE_REFRESH

    if not force and PAGE_TOKENS and (time.time() - LAST_PAGE_REFRESH < 600):
        return

    log("🔄 Loading pages...")
    PAGE_TOKENS = {}

    url = "https://graph.facebook.com/v25.0/me/accounts"
    params = {"access_token": USER_ACCESS_TOKEN, "limit": 100}

    while url:
        res = safe_get(url, params=params)
        if not res:
            log("❌ Failed to load pages")
            break

        data = res.json()

        if "error" in data:
            log(f"❌ Page load error → {data}")
            break

        for page in data.get("data", []):
            pid = page.get("id")
            ptoken = page.get("access_token")

            if pid and ptoken:
                PAGE_TOKENS[pid] = ptoken
                log(f"✅ {page.get('name','Unknown')} ({pid})")

        url = data.get("paging", {}).get("next")
        params = None

    LAST_PAGE_REFRESH = time.time()
    log(f"🔥 Total Pages → {len(PAGE_TOKENS)}")


# --------------------------------
# GET FORMS
# --------------------------------
def get_forms(page_id, token):
    forms = []

    url = f"https://graph.facebook.com/v25.0/{page_id}/leadgen_forms"
    params = {"access_token": token, "limit": 100}

    while url:
        res = safe_get(url, params=params)
        if not res:
            break

        data = res.json()

        if "error" in data:
            log(f"❌ Forms error → Page {page_id} → {data}")
            break

        forms.extend(data.get("data", []))

        url = data.get("paging", {}).get("next")
        params = None

    return forms


# --------------------------------
# FORM NAME CACHE
# --------------------------------
def get_form_name(form_id, token):
    if not form_id:
        return ""

    if form_id in FORM_NAME_CACHE:
        return FORM_NAME_CACHE[form_id]

    res = safe_get(
        f"https://graph.facebook.com/v25.0/{form_id}",
        params={"access_token": token, "fields": "name"}
    )

    if not res:
        return ""

    data = res.json()
    name = data.get("name", "")
    FORM_NAME_CACHE[form_id] = name
    return name


# --------------------------------
# FETCH LEAD
# --------------------------------
def fetch_lead(lead_id, token):
    res = safe_get(
        f"https://graph.facebook.com/v25.0/{lead_id}",
        params={
            "access_token": token,
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
                "field_data"
            ])
        }
    )

    if not res:
        return None

    data = res.json()

    if "error" in data:
        log(f"❌ Lead fetch error → {lead_id} → {data}")
        return None

    return data


# --------------------------------
# GET EXISTING LEAD IDS
# --------------------------------
def get_existing_lead_ids(lead_ids):
    if not lead_ids:
        return set()

    db = Database()
    ids_sql = ",".join([f"'{esc(i)}'" for i in lead_ids])

    sql = f"""
    SELECT LEAD_ID
    FROM dbo.LEADS
    WHERE LEAD_ID IN ({ids_sql})
    """

    status, rows = db.db_query(sql)

    if not status or not rows:
        return set()

    return {str(r[0]) for r in rows}


# --------------------------------
# SAVE CHECKPOINT
# --------------------------------
def save_checkpoint(form_id, cursor):
    db = Database()
    cursor = esc(cursor)

    sql = f"""
    MERGE LEAD_CHECKPOINT AS target
    USING (SELECT '{esc(form_id)}' AS FORM_ID) AS source
    ON target.FORM_ID = source.FORM_ID
    WHEN MATCHED THEN
        UPDATE SET CURSOR = '{cursor}', UPDATED_AT = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (FORM_ID, CURSOR, UPDATED_AT)
        VALUES ('{esc(form_id)}', '{cursor}', GETDATE());
    """

    db.db_update(sql)


def get_checkpoint(form_id):
    db = Database()

    sql = f"SELECT CURSOR FROM LEAD_CHECKPOINT WHERE FORM_ID='{esc(form_id)}'"
    status, rows = db.db_query(sql)

    if status and rows:
        return rows[0][0]

    return None


# --------------------------------
# BULK INSERT
# --------------------------------
def bulk_insert(leads):
    if not leads:
        return

    leads = [x for x in leads if x and x.get("id")]
    if not leads:
        return

    unique_map = {}
    for meta in leads:
        unique_map[meta["id"]] = meta

    leads = list(unique_map.values())

    existing = get_existing_lead_ids([x["id"] for x in leads])
    leads = [x for x in leads if x["id"] not in existing]

    if not leads:
        log("⏭ No new leads to insert")
        return

    db = Database()
    values = []

    for meta in leads:
        fields = extract(meta)

        travel_date = (
            fields.get("preferred_travel_date")
            or fields.get("preffered_travel_date")
            or fields.get("preferred_travel_month_or_date")
            or ""
        )

        phone = fields.get("phone_number") or fields.get("whatsapp_number") or ""

        form_id = esc(meta.get("form_id", ""))
        page_id = meta.get("_page_id", "")
        token = PAGE_TOKENS.get(page_id, "")
        form_name = esc(get_form_name(form_id, token))

        values.append(f"""
        (
            '{esc(meta.get("id"))}',
            '{esc(format_date(meta.get("created_time")))}',
            '{esc(meta.get("ad_id",""))}',
            '{esc(meta.get("ad_name",""))}',
            '{esc(meta.get("adset_id",""))}',
            '{esc(meta.get("adset_name",""))}',
            '{esc(meta.get("campaign_id",""))}',
            '{esc(meta.get("campaign_name",""))}',
            '{form_id}',
            '{form_name}',
            '{esc(meta.get("is_organic",""))}',
            '{esc(meta.get("platform",""))}',
            '{esc(fields.get("full_name",""))}',
            '{esc(phone)}',
            '{esc(fields.get("email",""))}',
            '{esc(fields.get("city",""))}',
            '{esc(travel_date)}',
            '{esc(fields.get("preferred_mode_of_contact",""))}',
            '{esc(fields.get("how_many_people_are_travelling_with",""))}',
            '{esc(json.dumps(meta))}',
            GETDATE(),
            GETDATE()
        )
        """)

    sql = f"""
    INSERT INTO dbo.LEADS (
        LEAD_ID, CREATED_TIME, AD_ID, AD_NAME, ADSET_ID, ADSET_NAME,
        CAMPAIGN_ID, CAMPAIGN_NAME, FORM_ID, FORM_NAME,
        IS_ORGANIC, PLATFORM, FULL_NAME, PHONE_NUMBER, EMAIL, CITY,
        PREFERRED_TRAVEL_DATE, PREFERRED_MODE_OF_CONTACT,
        HOW_MANY_PEOPLE_ARE_TRAVELLING_WITH,
        RAW_LEAD_JSON, CREATED_DATE, MODIFIED_DATE
    )
    VALUES {",".join(values)}
    """

    status, result = db.db_update(sql)

    if status:
        log(f"💾 Bulk Insert → {len(leads)} leads")
    else:
        log(f"❌ Bulk insert failed → {result}")


# --------------------------------
# DIRECT SAVE (FOR WEBHOOK)
# --------------------------------
def save_lead_direct(meta):
    if not meta:
        return

    try:
        bulk_insert([meta])
    except Exception as e:
        log(f"❌ Direct save error → {e}")


# --------------------------------
# BUFFER
# --------------------------------
def add_to_buffer(meta):
    if not meta:
        return

    with BUFFER_LOCK:
        INSERT_BUFFER.append(meta)

        if len(INSERT_BUFFER) >= BATCH_SIZE:
            batch = INSERT_BUFFER.copy()
            INSERT_BUFFER.clear()
            bulk_insert(batch)


def flush_buffer():
    with BUFFER_LOCK:
        if INSERT_BUFFER:
            batch = INSERT_BUFFER.copy()
            INSERT_BUFFER.clear()
            bulk_insert(batch)


def buffer_flusher():
    while True:
        try:
            flush_buffer()
        except Exception as e:
            log(f"❌ Buffer flusher error → {e}")
        time.sleep(BUFFER_FLUSH_SECONDS)


def start_buffer_flusher():
    global FLUSHER_STARTED

    if FLUSHER_STARTED:
        return

    threading.Thread(target=buffer_flusher, daemon=True).start()
    FLUSHER_STARTED = True
    log(f"🧼 Buffer flusher started → every {BUFFER_FLUSH_SECONDS}s")


# --------------------------------
# PROCESS LEAD
# --------------------------------
def process_lead_job(job):
    lead_id = job.get("lead_id")
    page_id = job.get("page_id")

    if not lead_id or not page_id:
        return

    token = PAGE_TOKENS.get(page_id)
    if not token:
        log(f"⚠ Missing page token → {page_id}")
        load_pages(force=True)
        token = PAGE_TOKENS.get(page_id)

    if not token:
        log(f"❌ No page token found → {page_id}")
        return

    meta = fetch_lead(lead_id, token)
    if not meta:
        return

    meta["_page_id"] = page_id
    add_to_buffer(meta)


# --------------------------------
# PROCESS LEAD DIRECT FOR WEBHOOK
# --------------------------------
def process_webhook_lead(lead_id, page_id):
    try:
        token = PAGE_TOKENS.get(page_id)

        if not token:
            load_pages(force=True)
            token = PAGE_TOKENS.get(page_id)

        if not token:
            log(f"❌ Webhook token missing → {page_id}")
            return

        meta = fetch_lead(lead_id, token)
        if not meta:
            return

        meta["_page_id"] = page_id

        # 🔥 DIRECT SAVE (like old code)
        save_lead_direct(meta)

        log(f"⚡ Instant webhook lead saved → {lead_id}")

    except Exception as e:
        log(f"❌ Webhook lead process error → {e}")


# --------------------------------
# WORKER LOOP
# --------------------------------
def worker_loop():
    while True:
        try:
            job = LEAD_QUEUE.get()

            if job is None:
                break

            process_lead_job(job)

        except Exception as e:
            log(f"❌ Worker error → {e}")
        finally:
            LEAD_QUEUE.task_done()


def start_workers():
    global WORKERS_STARTED

    if WORKERS_STARTED:
        return

    for _ in range(WORKER_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()

    WORKERS_STARTED = True
    log(f"👷 Workers started → {WORKER_COUNT}")


# --------------------------------
# INIT ENGINE
# --------------------------------
def init_engine():
    log("🚀 Initializing CRM Engine...")
    load_pages(force=True)
    start_workers()
    start_buffer_flusher()


# --------------------------------
# IMPORT ALL HISTORICAL
# --------------------------------
def import_all():
    global IMPORT_DONE

    log("🚀 HISTORICAL IMPORT START")
    load_pages(force=True)

    for page_id, token in PAGE_TOKENS.items():
        log(f"📘 Page → {page_id}")

        forms = get_forms(page_id, token)
        log(f"🧾 Forms → {len(forms)}")

        for form in forms:
            form_id = form.get("id")
            if not form_id:
                continue

            log(f"🚀 Fetch form → {form_id}")

            url = get_checkpoint(form_id)
            if not url:
                url = f"https://graph.facebook.com/v25.0/{form_id}/leads"

            params = {"access_token": token, "limit": 100}

            while url:
                res = safe_get(url, params=params)
                if not res:
                    break

                data = res.json()

                if "error" in data:
                    log(f"❌ Leads fetch error → Form {form_id} → {data}")
                    break

                leads = data.get("data", [])
                log(f"📦 Batch → {len(leads)}")

                for lead in leads:
                    lead_id = lead.get("id")
                    if lead_id:
                        LEAD_QUEUE.put({"lead_id": lead_id, "page_id": page_id})

                next_url = data.get("paging", {}).get("next")

                if next_url:
                    save_checkpoint(form_id, next_url)

                url = next_url
                params = None

    LEAD_QUEUE.join()
    flush_buffer()

    IMPORT_DONE = True
    log("✅ HISTORICAL IMPORT DONE")


# --------------------------------
# POLLING
# --------------------------------
def poll():
    while True:
        if not IMPORT_DONE:
            log("⏳ Waiting for historical import to finish before polling...")
            time.sleep(30)
            continue

        log("🔄 Polling backup running...")

        try:
            if not PAGE_TOKENS:
                load_pages(force=True)

            for page_id, token in PAGE_TOKENS.items():
                forms = get_forms(page_id, token)

                for form in forms:
                    form_id = form.get("id")
                    if not form_id:
                        continue

                    res = safe_get(
                        f"https://graph.facebook.com/v25.0/{form_id}/leads",
                        params={"access_token": token, "limit": 3}
                    )

                    if not res:
                        continue

                    data = res.json()

                    for lead in data.get("data", []):
                        lead_id = lead.get("id")
                        if lead_id:
                            LEAD_QUEUE.put({"lead_id": lead_id, "page_id": page_id})

            LEAD_QUEUE.join()
            flush_buffer()

        except Exception as e:
            log(f"❌ Poll error → {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


def start_polling():
    thread = threading.Thread(target=poll, daemon=True)
    thread.start()


# --------------------------------
# WEBHOOK VERIFY
# --------------------------------
@router.get("/webhook")
async def verify(request: Request):
    mode = request.query_params.get("hub.mode")
    verify_token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    log(f"🔐 Webhook verify request → mode={mode} token={verify_token}")

    if mode == "subscribe" and verify_token == VERIFY_TOKEN:
        log("✅ Webhook verified successfully")
        return int(challenge)

    log("❌ Webhook verification failed")
    return {"error": "Verification failed"}


# --------------------------------
# WEBHOOK RECEIVE
# --------------------------------
@router.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
        log(f"🔥 WEBHOOK RECEIVED → {json.dumps(body)}")

        for entry in body.get("entry", []):
            for change in entry.get("changes", []):

                if change.get("field") != "leadgen":
                    continue

                val = change.get("value", {})
                lead_id = val.get("leadgen_id")
                page_id = val.get("page_id")

                if lead_id and page_id:
                    thread = threading.Thread(
                        target=process_webhook_lead,
                        args=(lead_id, page_id),
                        daemon=True
                    )
                    thread.start()

                    log(f"📥 Instant webhook processing → {lead_id}")

    except Exception as e:
        log(f"❌ Webhook error → {e}")

    return {"status": "ok"}