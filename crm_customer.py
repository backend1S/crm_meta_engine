import time
import re
import logging
from collections import defaultdict

from database import Database

logger = logging.getLogger("crm_customer")


def log(msg):
    logger.info(msg)


def log_error(msg):
    logger.error(msg)


# =========================================================
# SAFE CLEANERS
# =========================================================
def clean_text(value, limit=255):
    try:
        if value is None:
            return ""
        value = str(value).strip()
        value = value.replace("\x00", "")
        value = value.replace("\n", " ").replace("\r", " ")
        value = " ".join(value.split())
        return value[:limit]
    except Exception:
        return ""


def safe_int(value, default=1, max_value=1000):
    try:
        num = int(value)
        if num < 0:
            return default
        if num > max_value:
            return default
        return num
    except:
        return default


def parse_pax(value):
    if not value:
        return 1
    try:
        nums = re.findall(r"\d+", str(value))
        if nums:
            return safe_int(nums[0], default=1, max_value=100)
    except:
        pass
    return 1


# =========================================================
# 🔥 NEW: CAMPAIGN → CITY (UPDATED ONLY)
# =========================================================
def extract_city_from_campaign(campaign_name):
    try:
        if not campaign_name:
            return ""

        campaign_name = clean_text(campaign_name, 255)

        # 🔥 handle both "_" and " "
        campaign_name = campaign_name.replace("_", " ")

        parts = campaign_name.split()

        if parts:
            return parts[0].strip().lower()

        return ""
    except:
        return ""


# =========================================================
# CONTACT MODE NORMALIZER
# =========================================================
def get_exact_contact_mode(value):
    value = clean_text(value, 50).lower()

    if not value:
        return ""

    replacements = {
        "whastapp": "whatsapp",
        "whatsapp": "whatsapp",
        "phone_call": "phone_call",
        "phone call": "phone_call",
        "call": "phone_call",
        "email": "email",
        "instagram": "instagram",
        "facebook": "facebook",
        "messenger": "messenger",
    }

    if value in replacements:
        return replacements[value]

    if "<test lead" in value or "dummy" in value or "test" in value:
        return "other"

    return clean_text(value, 50)


# =========================================================
# CACHE LOADERS
# =========================================================
def load_branch_map():
    db = Database()
    sql = "SELECT BRANCH_ID, CITY FROM BRANCH_MASTER"
    status, rows = db.db_query(sql)

    branch_map = []
    if status and rows:
        for row in rows:
            branch_id = clean_text(row[0], 20)
            branch_city = clean_text(row[1], 100).lower()
            if branch_id and branch_city:
                branch_map.append((branch_city, branch_id))

    return branch_map


def get_branch_id(city, branch_map):
    city = clean_text(city, 100).lower()

    if not city:
        return "B001"

    for branch_city, branch_id in branch_map:
        if branch_city in city or city in branch_city:
            return branch_id

    return "B001"


def load_cre_map():
    db = Database()

    sql = """
    SELECT EMPID, BRANCH_ID
    FROM SW_USER
    WHERE UPPER(LTRIM(RTRIM(ISNULL(ROLES, '')))) = 'CRE'
      AND (ISDISABLED IS NULL OR ISDISABLED = 0)
    ORDER BY BRANCH_ID, EMPID
    """

    status, rows = db.db_query(sql)

    cre_map = defaultdict(list)

    if status and rows:
        for row in rows:
            empid = clean_text(row[0], 50)
            branch_id = clean_text(row[1], 20)
            if empid and branch_id:
                cre_map[branch_id].append(empid)

    return cre_map


def load_last_cre_map():
    db = Database()

    sql = """
    SELECT BRANCH_ID, MAX(CUSTOMER_ID) AS LAST_CUSTOMER_ID
    FROM CRM_CUSTOMERS
    WHERE BRANCH_ID IS NOT NULL
      AND BRANCH_ID <> ''
    GROUP BY BRANCH_ID
    """

    status, rows = db.db_query(sql)

    last_cre_map = {}

    if status and rows:
        for row in rows:
            branch_id = clean_text(row[0], 20)

            sql2 = """
            SELECT TOP 1 CRE_ID
            FROM CRM_CUSTOMERS
            WHERE BRANCH_ID = ?
            ORDER BY CUSTOMER_ID DESC
            """
            s2, r2 = db.db_query(sql2, [branch_id])
            if s2 and r2:
                last_cre_map[branch_id] = clean_text(r2[0][0], 50)

    return last_cre_map


def get_next_cre_by_branch(branch_id, cre_map, last_cre_map):
    cre_list = cre_map.get(branch_id, [])

    if not cre_list:
        return None

    last_cre = last_cre_map.get(branch_id)

    if not last_cre or last_cre not in cre_list:
        chosen = cre_list[0]
        last_cre_map[branch_id] = chosen
        return chosen

    idx = cre_list.index(last_cre)
    next_idx = (idx + 1) % len(cre_list)
    chosen = cre_list[next_idx]
    last_cre_map[branch_id] = chosen
    return chosen


# =========================================================
# DUPLICATE CACHE
# =========================================================
def load_existing_customers():
    db = Database()

    sql = """
    SELECT PHONE_NUMBER, EMAIL
    FROM CRM_CUSTOMERS
    """

    status, rows = db.db_query(sql)

    phones = set()
    emails = set()

    if status and rows:
        for row in rows:
            phone = clean_text(row[0], 30)
            email = clean_text(row[1], 150).lower()

            if phone:
                phones.add(phone)
            if email:
                emails.add(email)

    return phones, emails


# =========================================================
# FETCH LEADS (KEEP OLD FILTER)
# =========================================================
def get_unprocessed_leads(limit=500):
    db = Database()

    sql = f"""
    SELECT TOP ({limit}) *
    FROM LEADS
    WHERE ISNULL(IS_PROCESSED, 0) = 0
      AND CREATED_DATE >= DATEADD(DAY, -30, GETDATE())
    ORDER BY ID ASC
    """

    status, rows = db.db_query(sql)

    return rows if status else []


# =========================================================
# BULK INSERT (UPDATED CREATED_DATE ONLY)
# =========================================================
def bulk_insert_customers(rows_to_insert):
    if not rows_to_insert:
        return 0

    db = Database()

    sql = """
    INSERT INTO CRM_CUSTOMERS
    (
        CUSTOMER_NAME,
        PHONE_NUMBER,
        EMAIL,
        CRE_ID,
        PACKAGE,
        MONTH,
        PAX,
        CONTACT_MODE,
        STATUS,
        REMARK,
        CREATED_DATE,
        MODIFIED_DATE,
        BRANCH_ID,
        PLATFORM
    )
    VALUES
    (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?
    )
    """

    status, result = db.db_executemany(sql, rows_to_insert)

    if status:
        log(f"👥 CRM Customers inserted → {len(rows_to_insert)}")
        return len(rows_to_insert)

    log_error(f"❌ CRM bulk insert failed → {result}")
    return 0


# =========================================================
# PROCESS LEADS (ONLY CHANGED LOGIC HERE)
# =========================================================
def process_leads():
    rows = get_unprocessed_leads(limit=500)

    if not rows:
        return

    branch_map = load_branch_map()
    cre_map = load_cre_map()
    last_cre_map = load_last_cre_map()
    existing_phones, existing_emails = load_existing_customers()

    rows_to_insert = []
    processed_lead_ids = []

    for row in rows:
        try:
            lead_id = clean_text(row[1], 100)
            name = clean_text(row[13], 150)

            phone = clean_text(row[14], 30)
            email = clean_text(row[15], 150).lower()

            # 🔥 STRICT NULL CHECK
            if phone == "" and email == "":
                processed_lead_ids.append(lead_id)
                continue

            # 🔥 NEW LOGIC
            campaign_name = clean_text(row[8], 255)
            city_from_lead = clean_text(row[16], 100)

            campaign_city = extract_city_from_campaign(campaign_name)
            final_city = campaign_city if campaign_city else city_from_lead

            branch_id = get_branch_id(final_city, branch_map)
            cre_id = get_next_cre_by_branch(branch_id, cre_map, last_cre_map)

            month = clean_text(row[17], 100)
            contact_mode = get_exact_contact_mode(row[18])
            pax = parse_pax(row[19])
            platform = clean_text(row[12], 50)

            created_time = row[2]  # 🔥 FIX

            # duplicate check
            if (phone and phone in existing_phones) or (email and email in existing_emails):
                processed_lead_ids.append(lead_id)
                continue

            rows_to_insert.append((
                name,
                phone,
                email,
                cre_id if cre_id else "",
                "",
                month,
                pax,
                contact_mode,
                "Pending",
                "",
                created_time,
                branch_id,
                platform
            ))

            processed_lead_ids.append(lead_id)

            if phone:
                existing_phones.add(phone)
            if email:
                existing_emails.add(email)

        except Exception as e:
            log_error(f"❌ Lead process failed → {e}")

    inserted = bulk_insert_customers(rows_to_insert)

    if processed_lead_ids:
        db = Database()
        placeholders = ",".join(["?"] * len(processed_lead_ids))
        sql = f"""
        UPDATE LEADS
        SET IS_PROCESSED = 1,
            MODIFIED_DATE = GETDATE()
        WHERE LEAD_ID IN ({placeholders})
        """
        db.db_update(sql, processed_lead_ids)

    log(f"✅ CRM sync processed → leads={len(rows)} | inserted={inserted}")


# =========================================================
# MAIN LOOP
# =========================================================
def start_crm_customer_sync():
    log("🚀 CRM Customer Sync Started")

    while True:
        try:
            process_leads()
        except Exception as e:
            log_error(f"❌ CRM Sync Error → {e}")

        time.sleep(10)