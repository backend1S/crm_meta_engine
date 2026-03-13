import time
import re
from database import Database


# ------------------------------------------------
# LOGGER
# ------------------------------------------------
def log(msg):
    print(msg, flush=True)


# ------------------------------------------------
# CLEAN TEXT
# ------------------------------------------------
def clean_text(value, limit=100):

    if not value:
        return ""

    value = str(value).replace("'", "''").strip()

    return value[:limit]


# ------------------------------------------------
# PARSE PAX
# ------------------------------------------------
def parse_pax(value):

    if not value:
        return 1

    try:
        nums = re.findall(r"\d+", str(value))

        if nums:
            return int(nums[0])

    except:
        pass

    return 1


# ------------------------------------------------
# NORMALIZE CONTACT MODE
# ------------------------------------------------
def normalize_contact_mode(value):

    if not value:
        return "Call"

    value = str(value).lower()

    if "whats" in value:
        return "Whatsup"

    if "call" in value:
        return "Call"

    if "email" in value:
        return "Email"

    return "Call"


# ------------------------------------------------
# GET BRANCH
# ------------------------------------------------
def get_branch_id(city):

    db = Database()

    city = clean_text(city).lower()

    if not city:
        return "B001"

    sql = "SELECT BRANCH_ID,CITY FROM BRANCH_MASTER"

    status, rows = db.db_query(sql)

    if not status or not rows:
        return "B001"

    for row in rows:

        branch_id = row[0]
        branch_city = str(row[1]).lower()

        if branch_city in city or city in branch_city:
            return branch_id

    return "B001"


# ------------------------------------------------
# CHECK DUPLICATE
# ------------------------------------------------
def customer_exists(phone, email):

    db = Database()

    sql = f"""
    SELECT TOP 1 CUSTOMER_ID
    FROM CRM_CUSTOMERS
    WHERE PHONE_NUMBER='{phone}'
       OR EMAIL='{email}'
    """

    status, rows = db.db_query(sql)

    if status and rows:
        return True

    return False


# ------------------------------------------------
# INSERT CUSTOMER
# ------------------------------------------------
def insert_customer(row):

    try:

        db = Database()

        name = clean_text(row[13], 80)
        phone = clean_text(row[14], 20)
        email = clean_text(row[15], 80)
        city = clean_text(row[16], 50)
        month = clean_text(row[17], 30)

        contact_mode = normalize_contact_mode(row[18])

        pax = parse_pax(row[19])

        branch_id = get_branch_id(city)

        log(f"Lead → {name} | {phone}")

        if not phone and not email:
            return

        if customer_exists(phone, email):

            log(f"⚠ Duplicate skipped → {phone}")

            return

        sql = f"""
        INSERT INTO CRM_CUSTOMERS
        (
            CUSTOMER_NAME,
            PHONE_NUMBER,
            EMAIL,
            PACKAGE,
            MONTH,
            PAX,
            CONTACT_MODE,
            STATUS,
            CREATED_DATE,
            MODIFIED_DATE,
            BRANCH_ID
        )
        VALUES
        (
            '{name}',
            '{phone}',
            '{email}',
            '',
            '{month}',
            {pax},
            '{contact_mode}',
            'Call Back',
            GETDATE(),
            GETDATE(),
            '{branch_id}'
        )
        """

        status, result = db.db_update(sql)

        if status:

            log(f"👤 CRM Customer Created → {name}")

        else:

            log(f"❌ Insert error → {result}")

    except Exception as e:

        log(f"❌ Insert error → {e}")


# ------------------------------------------------
# PROCESS ONLY NEW LEADS
# ------------------------------------------------
def process_leads():

    db = Database()

    sql = """
    SELECT *
    FROM LEADS
    WHERE CREATED_DATE >= DATEADD(SECOND,-20,GETDATE())
    """

    status, rows = db.db_query(sql)

    if not status or not rows:
        return

    for row in rows:

        insert_customer(row)


# ------------------------------------------------
# CONTINUOUS SYNC
# ------------------------------------------------
def start_crm_customer_sync():

    log("🚀 CRM Customer Sync Started")

    while True:

        try:

            process_leads()

        except Exception as e:

            log(f"❌ CRM Sync Error → {e}")

        time.sleep(5)