import os
import json
import time
import queue
import threading
import logging
import requests

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from dotenv import load_dotenv

from database import Database

load_dotenv()

# =========================================================
# SIMPLE LOGGER
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("crm")


def log(msg):
    logger.info(msg)


def log_error(msg):
    logger.error(msg)


# =========================================================
# ROUTER
# =========================================================
router = APIRouter(tags=["CRM"])


# =========================================================
# ENV
# =========================================================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "").strip()

if not VERIFY_TOKEN:
    log("⚠ VERIFY_TOKEN missing")

if not USER_ACCESS_TOKEN:
    log("⚠ USER_ACCESS_TOKEN missing")


# =========================================================
# SETTINGS
# =========================================================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
BUFFER_FLUSH_SECONDS = int(os.getenv("BUFFER_FLUSH_SECONDS", "10"))

# 🔥 Parallel speed settings
PAGE_FETCH_THREADS = int(os.getenv("PAGE_FETCH_THREADS", "10"))
FORM_FETCH_THREADS = int(os.getenv("FORM_FETCH_THREADS", "20"))
LEAD_FETCH_THREADS = int(os.getenv("LEAD_FETCH_THREADS", "30"))

# DB SAFE LENGTHS
MAX_FORM_NAME = 255
MAX_NAME = 255
MAX_PHONE = 50
MAX_EMAIL = 255
MAX_CITY = 150
MAX_TRAVEL_DATE = 100
MAX_CONTACT_MODE = 100
MAX_TRAVELLERS = 100
MAX_PLATFORM = 50
MAX_AD_NAME = 255
MAX_ADSET_NAME = 255
MAX_CAMPAIGN_NAME = 255


# =========================================================
# RUNTIME
# =========================================================
PAGE_TOKENS = {}
PAGE_NAMES = {}
FORM_NAME_CACHE = {}

LEAD_QUEUE = queue.Queue(maxsize=50000)       # backup import/poll queue
WEBHOOK_QUEUE = queue.Queue(maxsize=50000)    # 🔥 first priority webhook queue

INSERT_BUFFER = []
BUFFER_LOCK = threading.Lock()

WORKERS_STARTED = False
IMPORT_DONE = False
LAST_PAGE_REFRESH = 0
FLUSHER_STARTED = False
POLLING_STARTED = False


# =========================================================
# HELPERS
# =========================================================
def clean_text(value, max_len=None):
    try:
        if value is None:
            value = ""
        value = str(value).strip()
        value = value.replace("\x00", "")
        value = value.replace("\n", " ").replace("\r", " ")
        value = " ".join(value.split())
        if max_len and len(value) > max_len:
            return value[:max_len]
        return value
    except Exception:
        return ""


def format_date(meta_date):
    try:
        dt = datetime.strptime(meta_date, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def extract(meta):
    fields = {}
    for f in meta.get("field_data", []):
        name = clean_text(f.get("name", "")).replace("?", "").lower()
        values = f.get("values", [])
        value = values[0] if values else ""
        fields[name] = clean_text(value)
    return fields


# =========================================================
# SAFE HTTP
# =========================================================
def safe_get(url, params=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if res.status_code == 200:
                return res
            log(f"⚠ Meta GET failed [{res.status_code}] (try {attempt}/{MAX_RETRIES})")
        except Exception as e:
            log(f"⚠ Meta GET retry {attempt}/{MAX_RETRIES}: {e}")
        time.sleep(1)
    return None


def safe_post(url, data=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
            if res.status_code in [200, 201]:
                return res
            log(f"⚠ Meta POST failed [{res.status_code}] (try {attempt}/{MAX_RETRIES})")
        except Exception as e:
            log(f"⚠ Meta POST retry {attempt}/{MAX_RETRIES}: {e}")
        time.sleep(1)
    return None


# =========================================================
# LOAD PAGES
# =========================================================
def load_pages(force=False):
    global PAGE_TOKENS, PAGE_NAMES, LAST_PAGE_REFRESH

    if not force and PAGE_TOKENS and (time.time() - LAST_PAGE_REFRESH < 600):
        return

    if not USER_ACCESS_TOKEN:
        log_error("❌ USER_ACCESS_TOKEN missing")
        return

    log("🔄 Loading Meta pages...")

    PAGE_TOKENS = {}
    PAGE_NAMES = {}

    url = "https://graph.facebook.com/v25.0/me/accounts"
    params = {"access_token": USER_ACCESS_TOKEN, "limit": 100}

    while url:
        res = safe_get(url, params=params)
        if not res:
            log_error("❌ Failed to load pages")
            break

        data = res.json()

        if "error" in data:
            log_error(f"❌ Page load error: {data}")
            break

        for page in data.get("data", []):
            pid = clean_text(page.get("id"))
            ptoken = clean_text(page.get("access_token"))
            pname = clean_text(page.get("name"))

            if pid and ptoken:
                PAGE_TOKENS[pid] = ptoken
                PAGE_NAMES[pid] = pname
                log(f"📘 Page Loaded → {pname}")

        url = data.get("paging", {}).get("next")
        params = None

    LAST_PAGE_REFRESH = time.time()
    log(f"✅ Meta Pages Ready → {len(PAGE_TOKENS)} page(s)")


# =========================================================
# SUBSCRIBE ALL PAGES
# =========================================================
def subscribe_all_pages():
    if not PAGE_TOKENS:
        return

    log("🔔 Ensuring pages are subscribed...")

    for page_id, token in PAGE_TOKENS.items():
        try:
            url = f"https://graph.facebook.com/v25.0/{page_id}/subscribed_apps"
            data = {
                "subscribed_fields": "leadgen,leadgen_update",
                "access_token": token
            }

            res = safe_post(url, data=data)
            if res:
                log(f"✅ Webhook subscribed → {PAGE_NAMES.get(page_id, page_id)}")
            else:
                log(f"⚠ Subscribe failed → {PAGE_NAMES.get(page_id, page_id)}")

        except Exception as e:
            log_error(f"❌ Subscribe error ({page_id}) → {e}")


# =========================================================
# TOKEN HELPERS
# =========================================================
def get_page_token(page_id):
    token = PAGE_TOKENS.get(str(page_id))
    if token:
        return token

    load_pages(force=True)
    subscribe_all_pages()
    return PAGE_TOKENS.get(str(page_id), "")


# =========================================================
# FORMS
# =========================================================
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
            log_error(f"❌ Forms fetch error for page {page_id}")
            break

        forms.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = None

    return forms


def fetch_forms_for_page(page_id, token):
    try:
        forms = get_forms(page_id, token)
        return {
            "page_id": page_id,
            "forms": forms
        }
    except Exception as e:
        log_error(f"❌ fetch_forms_for_page failed → {page_id} | {e}")
        return {
            "page_id": page_id,
            "forms": []
        }


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
    name = clean_text(data.get("name", ""), MAX_FORM_NAME)
    FORM_NAME_CACHE[form_id] = name
    return name


def get_form_leads_page(form_id, token, limit=100):
    """
    Fetch latest leads for one form (single page request only for speed).
    Used in polling.
    """
    try:
        res = safe_get(
            f"https://graph.facebook.com/v25.0/{form_id}/leads",
            params={"access_token": token, "limit": limit}
        )

        if not res:
            return []

        data = res.json()
        if "error" in data:
            log_error(f"❌ Lead list fetch failed for form {form_id} → {data}")
            return []

        return data.get("data", [])

    except Exception as e:
        log_error(f"❌ get_form_leads_page failed → {form_id} | {e}")
        return []


def get_all_form_leads_full(form_id, token):
    """
    Full historical lead fetch for one form.
    Used in historical import.
    """
    all_leads = []
    try:
        url = f"https://graph.facebook.com/v25.0/{form_id}/leads"
        params = {"access_token": token, "limit": 100}

        while url:
            res = safe_get(url, params=params)
            if not res:
                break

            data = res.json()
            if "error" in data:
                log_error(f"❌ Full lead fetch failed for form {form_id} → {data}")
                break

            all_leads.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
            params = None

    except Exception as e:
        log_error(f"❌ get_all_form_leads_full failed → {form_id} | {e}")

    return all_leads


# =========================================================
# FETCH LEAD
# =========================================================
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
        log_error(f"❌ Lead fetch failed → {lead_id} | {data}")
        return None

    return data


# =========================================================
# VERY FAST SINGLE INSERT (WEBHOOK FIRST)
# =========================================================
def insert_one_lead_fast(meta):
    try:
        if not meta or not meta.get("id"):
            return False

        db = Database()
        fields = extract(meta)

        travel_date = (
            fields.get("preferred_travel_date")
            or fields.get("preffered_travel_date")
            or fields.get("preferred_travel_month_or_date")
            or fields.get("month")
            or ""
        )

        phone = (
            fields.get("phone_number")
            or fields.get("whatsapp_number")
            or fields.get("mobile_number")
            or ""
        )

        form_id = clean_text(meta.get("form_id"))
        page_id = clean_text(meta.get("_page_id"))
        token = PAGE_TOKENS.get(page_id, "")
        form_name = get_form_name(form_id, token)

        is_organic = 1 if str(meta.get("is_organic", "")).lower() in ["true", "1"] else 0

        sql = """
        IF NOT EXISTS (SELECT 1 FROM dbo.LEADS WHERE LEAD_ID = ?)
        BEGIN
            INSERT INTO dbo.LEADS (
                LEAD_ID, CREATED_TIME, AD_ID, AD_NAME, ADSET_ID, ADSET_NAME,
                CAMPAIGN_ID, CAMPAIGN_NAME, FORM_ID, FORM_NAME,
                IS_ORGANIC, PLATFORM, FULL_NAME, PHONE_NUMBER, EMAIL, CITY,
                PREFERRED_TRAVEL_DATE, PREFERRED_MODE_OF_CONTACT,
                HOW_MANY_PEOPLE_ARE_TRAVELLING_WITH,
                RAW_LEAD_JSON
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        END
        """

        params = [
            clean_text(meta.get("id")),
            clean_text(meta.get("id")),
            format_date(meta.get("created_time")),
            clean_text(meta.get("ad_id")),
            clean_text(meta.get("ad_name"), MAX_AD_NAME),
            clean_text(meta.get("adset_id")),
            clean_text(meta.get("adset_name"), MAX_ADSET_NAME),
            clean_text(meta.get("campaign_id")),
            clean_text(meta.get("campaign_name"), MAX_CAMPAIGN_NAME),
            form_id,
            clean_text(form_name, MAX_FORM_NAME),
            is_organic,
            clean_text(meta.get("platform"), MAX_PLATFORM),
            clean_text(fields.get("full_name"), MAX_NAME),
            clean_text(phone, MAX_PHONE),
            clean_text(fields.get("email"), MAX_EMAIL),
            clean_text(fields.get("city"), MAX_CITY),
            clean_text(travel_date, MAX_TRAVEL_DATE),
            clean_text(fields.get("preferred_mode_of_contact"), MAX_CONTACT_MODE),
            clean_text(fields.get("how_many_people_are_travelling_with"), MAX_TRAVELLERS),
            json.dumps(meta, ensure_ascii=False),
        ]

        status, result = db.db_update(sql, params)

        if status:
            log(f"⚡ Lead inserted instantly → {meta.get('id')}")
            return True

        log_error(f"❌ Instant insert failed → {result}")
        return False

    except Exception as e:
        log_error(f"❌ insert_one_lead_fast failed → {e}")
        return False


# =========================================================
# BULK INSERT (BACKUP PATH)
# =========================================================
def bulk_insert(leads):
    if not leads:
        return

    leads = [x for x in leads if x and x.get("id")]
    if not leads:
        return

    db = Database()
    rows = []

    for meta in leads:
        try:
            fields = extract(meta)

            travel_date = (
                fields.get("preferred_travel_date")
                or fields.get("preffered_travel_date")
                or fields.get("preferred_travel_month_or_date")
                or fields.get("month")
                or ""
            )

            phone = (
                fields.get("phone_number")
                or fields.get("whatsapp_number")
                or fields.get("mobile_number")
                or ""
            )

            form_id = clean_text(meta.get("form_id"))
            page_id = clean_text(meta.get("_page_id"))
            token = PAGE_TOKENS.get(page_id, "")
            form_name = get_form_name(form_id, token)

            is_organic = 1 if str(meta.get("is_organic", "")).lower() in ["true", "1"] else 0

            row = (
                clean_text(meta.get("id")),
                format_date(meta.get("created_time")),
                clean_text(meta.get("ad_id")),
                clean_text(meta.get("ad_name"), MAX_AD_NAME),
                clean_text(meta.get("adset_id")),
                clean_text(meta.get("adset_name"), MAX_ADSET_NAME),
                clean_text(meta.get("campaign_id")),
                clean_text(meta.get("campaign_name"), MAX_CAMPAIGN_NAME),
                form_id,
                clean_text(form_name, MAX_FORM_NAME),
                is_organic,
                clean_text(meta.get("platform"), MAX_PLATFORM),
                clean_text(fields.get("full_name"), MAX_NAME),
                clean_text(phone, MAX_PHONE),
                clean_text(fields.get("email"), MAX_EMAIL),
                clean_text(fields.get("city"), MAX_CITY),
                clean_text(travel_date, MAX_TRAVEL_DATE),
                clean_text(fields.get("preferred_mode_of_contact"), MAX_CONTACT_MODE),
                clean_text(fields.get("how_many_people_are_travelling_with"), MAX_TRAVELLERS),
                json.dumps(meta, ensure_ascii=False),
            )

            rows.append(row)

        except Exception as e:
            log_error(f"❌ Row build failed for lead {meta.get('id')} → {e}")

    if not rows:
        return

    sql = """
    INSERT INTO dbo.LEADS (
        LEAD_ID, CREATED_TIME, AD_ID, AD_NAME, ADSET_ID, ADSET_NAME,
        CAMPAIGN_ID, CAMPAIGN_NAME, FORM_ID, FORM_NAME,
        IS_ORGANIC, PLATFORM, FULL_NAME, PHONE_NUMBER, EMAIL, CITY,
        PREFERRED_TRAVEL_DATE, PREFERRED_MODE_OF_CONTACT,
        HOW_MANY_PEOPLE_ARE_TRAVELLING_WITH,
        RAW_LEAD_JSON
    )
    SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.LEADS WHERE LEAD_ID = ?
    )
    """

    fixed_rows = []
    for r in rows:
        fixed_rows.append(r + (r[0],))

    try:
        status, result = db.db_executemany(sql, fixed_rows)
        if status:
            log(f"💾 Saved {len(rows)} lead(s)")
        else:
            log_error(f"❌ DB insert failed → {result}")
    except Exception as e:
        log_error(f"❌ Bulk insert exception → {e}")


# =========================================================
# BUFFER / BATCH (BACKUP ONLY)
# =========================================================
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
            log_error(f"❌ Buffer flush error → {e}")
        time.sleep(BUFFER_FLUSH_SECONDS)


def start_buffer_flusher():
    global FLUSHER_STARTED
    if FLUSHER_STARTED:
        return
    threading.Thread(target=buffer_flusher, daemon=True).start()
    FLUSHER_STARTED = True
    log(f"🧼 Buffer auto-save every {BUFFER_FLUSH_SECONDS}s")


# =========================================================
# WORKER PROCESSING (BACKUP ONLY)
# =========================================================
def process_lead_job(job):
    lead_id = job.get("lead_id")
    page_id = clean_text(job.get("page_id"))

    if not lead_id or not page_id:
        return

    token = get_page_token(page_id)
    if not token:
        return

    meta = fetch_lead(lead_id, token)
    if not meta:
        return

    meta["_page_id"] = page_id
    add_to_buffer(meta)


def process_webhook_lead_immediate(lead_id, page_id):
    """
    FASTEST PATH:
    webhook queue → fetch → insert immediately
    """
    try:
        page_id = clean_text(page_id)
        token = get_page_token(page_id)

        if not token:
            log_error(f"❌ Webhook token missing for page {page_id}")
            return

        meta = fetch_lead(lead_id, token)
        if not meta:
            log_error(f"❌ Webhook fetch failed for lead {lead_id}")
            return

        meta["_page_id"] = page_id

        ok = insert_one_lead_fast(meta)

        if ok:
            log(f"🚀 WEBHOOK FIRST PRIORITY SAVED → {lead_id}")
        else:
            log_error(f"❌ Webhook immediate save failed → {lead_id}")

    except Exception as e:
        log_error(f"❌ Webhook processing failed → {e}")


# =========================================================
# WORKERS
# =========================================================
def worker_loop():
    while True:
        job = None
        try:
            job = LEAD_QUEUE.get()
            if job is None:
                continue
            process_lead_job(job)
        except Exception as e:
            log_error(f"❌ Worker failed → {e}")
        finally:
            if job is not None:
                LEAD_QUEUE.task_done()


def webhook_worker_loop():
    while True:
        job = None
        try:
            job = WEBHOOK_QUEUE.get()
            if not job:
                continue

            lead_id = job.get("lead_id")
            page_id = job.get("page_id")

            if lead_id and page_id:
                process_webhook_lead_immediate(lead_id, page_id)

        except Exception as e:
            log_error(f"❌ Webhook worker failed → {e}")
        finally:
            if job is not None:
                WEBHOOK_QUEUE.task_done()


def start_workers():
    global WORKERS_STARTED

    if WORKERS_STARTED:
        return

    # normal workers
    for _ in range(WORKER_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()

    # 🔥 dedicated webhook priority worker
    threading.Thread(target=webhook_worker_loop, daemon=True).start()

    WORKERS_STARTED = True
    log(f"👷 Workers started → {WORKER_COUNT} + 1 webhook priority worker")


# =========================================================
# INIT ENGINE
# =========================================================
def init_engine():
    log("🚀 Initializing CRM Engine...")
    load_pages(force=True)
    subscribe_all_pages()
    start_workers()
    start_buffer_flusher()
    log("✅ CRM Engine ready")


# =========================================================
# HISTORICAL IMPORT (PARALLEL)
# =========================================================
def import_all():
    global IMPORT_DONE

    try:
        log("📥 Historical import started (parallel mode)")

        load_pages(force=True)
        subscribe_all_pages()

        page_items = list(PAGE_TOKENS.items())
        all_page_forms = []

        # -----------------------------------------
        # STEP 1: Fetch all forms from all pages in parallel
        # -----------------------------------------
        with ThreadPoolExecutor(max_workers=PAGE_FETCH_THREADS) as executor:
            futures = [
                executor.submit(fetch_forms_for_page, page_id, token)
                for page_id, token in page_items
            ]

            for future in as_completed(futures):
                result = future.result()
                page_id = result["page_id"]
                forms = result["forms"]

                page_name = PAGE_NAMES.get(page_id, "Unknown")
                log(f"📘 Page Loaded → {page_name} | Forms → {len(forms)}")

                for form in forms:
                    form_id = form.get("id")
                    if form_id:
                        all_page_forms.append((page_id, PAGE_TOKENS.get(page_id), form_id))

        log(f"🧾 Total forms discovered → {len(all_page_forms)}")

        # -----------------------------------------
        # STEP 2: Fetch all leads from all forms in parallel
        # -----------------------------------------
        def import_one_form(page_id, token, form_id):
            leads = get_all_form_leads_full(form_id, token)
            return {
                "page_id": page_id,
                "form_id": form_id,
                "leads": leads
            }

        with ThreadPoolExecutor(max_workers=FORM_FETCH_THREADS) as executor:
            futures = [
                executor.submit(import_one_form, page_id, token, form_id)
                for page_id, token, form_id in all_page_forms
            ]

            for future in as_completed(futures):
                result = future.result()
                page_id = result["page_id"]
                form_id = result["form_id"]
                leads = result["leads"]

                log(f"📄 Imported form → {form_id} | Leads → {len(leads)}")

                for lead in leads:
                    lead_id = lead.get("id")
                    if lead_id:
                        LEAD_QUEUE.put({"lead_id": lead_id, "page_id": page_id})

        LEAD_QUEUE.join()
        flush_buffer()

        IMPORT_DONE = True
        log("✅ Historical import completed (parallel)")

    except Exception as e:
        log_error(f"❌ Historical import failed → {e}")


# =========================================================
# POLLING BACKUP (PARALLEL)
# =========================================================
def poll():
    while True:
        try:
            if not IMPORT_DONE:
                time.sleep(30)
                continue

            log("🔄 Polling backup check (parallel mode)...")

            load_pages(force=True)

            page_items = list(PAGE_TOKENS.items())
            all_page_forms = []

            # -----------------------------------------
            # STEP 1: Fetch all forms from all pages in parallel
            # -----------------------------------------
            with ThreadPoolExecutor(max_workers=PAGE_FETCH_THREADS) as executor:
                futures = [
                    executor.submit(fetch_forms_for_page, page_id, token)
                    for page_id, token in page_items
                ]

                for future in as_completed(futures):
                    result = future.result()
                    page_id = result["page_id"]
                    forms = result["forms"]

                    page_name = PAGE_NAMES.get(page_id, "Unknown")
                    log(f"📘 Poll page → {page_name} | Forms → {len(forms)}")

                    for form in forms:
                        form_id = form.get("id")
                        if form_id:
                            all_page_forms.append((page_id, PAGE_TOKENS.get(page_id), form_id))

            log(f"🧾 Poll total forms → {len(all_page_forms)}")

            # -----------------------------------------
            # STEP 2: Fetch latest leads from all forms in parallel
            # -----------------------------------------
            def poll_one_form(page_id, token, form_id):
                leads = get_form_leads_page(form_id, token, limit=100)
                return {
                    "page_id": page_id,
                    "form_id": form_id,
                    "leads": leads
                }

            with ThreadPoolExecutor(max_workers=LEAD_FETCH_THREADS) as executor:
                futures = [
                    executor.submit(poll_one_form, page_id, token, form_id)
                    for page_id, token, form_id in all_page_forms
                ]

                for future in as_completed(futures):
                    result = future.result()
                    page_id = result["page_id"]
                    form_id = result["form_id"]
                    leads = result["leads"]

                    if leads:
                        log(f"📄 Poll form → {form_id} | Leads → {len(leads)}")

                    for lead in leads:
                        lead_id = lead.get("id")
                        if lead_id:
                            LEAD_QUEUE.put({"lead_id": lead_id, "page_id": page_id})

            LEAD_QUEUE.join()
            flush_buffer()

        except Exception as e:
            log_error(f"❌ Polling error → {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


def start_polling():
    global POLLING_STARTED

    if POLLING_STARTED:
        return

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    POLLING_STARTED = True


# =========================================================
# HEALTH
# =========================================================
@router.get("/api/health/crm")
def crm_health():
    return {
        "status": "ok",
        "pages_loaded": len(PAGE_TOKENS),
        "workers_started": WORKERS_STARTED,
        "import_done": IMPORT_DONE,
        "buffer_size": len(INSERT_BUFFER),
        "queue_size": LEAD_QUEUE.qsize(),
        "webhook_queue_size": WEBHOOK_QUEUE.qsize()
    }


# =========================================================
# META WEBHOOK VERIFY
# =========================================================
@router.get("/api/switrus_engine")
async def verify(request: Request):
    mode = request.query_params.get("hub.mode")
    verify_token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and verify_token == VERIFY_TOKEN:
        log("✅ Webhook verified")
        return PlainTextResponse(content=challenge or "", status_code=200)

    log_error("❌ Webhook verification failed")
    return JSONResponse(content={"error": "Verification failed"}, status_code=403)


# =========================================================
# META WEBHOOK RECEIVE (FAST ACK)
# =========================================================
@router.post("/api/switrus_engine")
async def webhook(request: Request):
    try:
        raw_body = await request.body()

        try:
            body = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            log_error(f"❌ Invalid webhook JSON → {e}")
            return {"status": "bad_json"}

        events_found = 0

        for entry in body.get("entry", []):
            for change in entry.get("changes", []):
                field_name = change.get("field")

                if field_name not in ["leadgen", "leadgen_update"]:
                    continue

                val = change.get("value", {})
                lead_id = val.get("leadgen_id")
                page_id = val.get("page_id")

                if lead_id and page_id:
                    events_found += 1

                    # 🔥 ACK FAST → queue immediately
                    try:
                        WEBHOOK_QUEUE.put_nowait({
                            "lead_id": lead_id,
                            "page_id": page_id
                        })
                        log(f"⚡ Webhook queued instantly → {lead_id}")
                    except Exception as qe:
                        log_error(f"❌ Webhook queue failed → {lead_id} | {qe}")

        if events_found == 0:
            log("ℹ Webhook received but no lead events found")
        else:
            log(f"📥 Webhook ACK sent immediately for {events_found} lead event(s)")

    except Exception as e:
        log_error(f"❌ Webhook handler failed → {e}")

    return {"status": "ok"}