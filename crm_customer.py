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
def clean_text(value, limit=255):
    if value is None:
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
# EXACT CONTACT MODE
# Save exactly from LEADS.PREFERRED_MODE_OF_CONTACT
# ------------------------------------------------
def get_exact_contact_mode(value):
    return clean_text(value, 100)


# ------------------------------------------------
# GET BRANCH ID FROM CITY
# ------------------------------------------------
def get_branch_id(city):
    db = Database()

    city = clean_text(city, 100).lower()

    if not city:
        return "B001"

    sql = "SELECT BRANCH_ID, CITY FROM BRANCH_MASTER"
    status, rows = db.db_query(sql)

    if not status or not rows:
        return "B001"

    for row in rows:
        branch_id = clean_text(row[0], 20)
        branch_city = clean_text(row[1], 100).lower()

        if branch_city in city or city in branch_city:
            return branch_id

    return "B001"


# ------------------------------------------------
# GET NEXT CRE BY BRANCH (ROUND ROBIN)
# SW_USER.BRANCH_ID = CRM_CUSTOMERS.BRANCH_ID
# SW_USER.ROLES = 'CRE'
# ------------------------------------------------
def get_next_cre_by_branch(branch_id):
    db = Database()

    branch_id = clean_text(branch_id, 20)

    if not branch_id:
        return None

    sql_cre = f"""
    SELECT EMPID
    FROM SW_USER
    WHERE BRANCH_ID = '{branch_id}'
      AND UPPER(LTRIM(RTRIM(ISNULL(ROLES, '')))) = 'CRE'
      AND (ISDISABLED IS NULL OR ISDISABLED = 0)
    ORDER BY EMPID
    """

    status, rows = db.db_query(sql_cre)

    if not status or not rows:
        return None

    cre_list = [clean_text(r[0], 50) for r in rows if r[0]]

    if not cre_list:
        return None

    sql_last = f"""
    SELECT TOP 1 CRE_ID
    FROM CRM_CUSTOMERS
    WHERE BRANCH_ID = '{branch_id}'
      AND CRE_ID IS NOT NULL
      AND CRE_ID <> ''
    ORDER BY CUSTOMER_ID DESC
    """

    status, last_rows = db.db_query(sql_last)

    if not status or not last_rows:
        return cre_list[0]

    last_cre = clean_text(last_rows[0][0], 50)

    if last_cre not in cre_list:
        return cre_list[0]

    idx = cre_list.index(last_cre)
    next_idx = (idx + 1) % len(cre_list)

    return cre_list[next_idx]


# ------------------------------------------------
# CHECK DUPLICATE CUSTOMER
# ------------------------------------------------
def customer_exists(phone, email):
    db = Database()

    phone = clean_text(phone, 30)
    email = clean_text(email, 150)

    if not phone and not email:
        return False

    conditions = []

    if phone:
        conditions.append(f"PHONE_NUMBER='{phone}'")

    if email:
        conditions.append(f"EMAIL='{email}'")

    sql = f"""
    SELECT TOP 1 CUSTOMER_ID
    FROM CRM_CUSTOMERS
    WHERE {" OR ".join(conditions)}
    """

    status, rows = db.db_query(sql)

    if status and rows:
        return True

    return False


# ------------------------------------------------
# CHECK IF LEAD ALREADY PROCESSED
# ------------------------------------------------
def is_lead_processed(lead_id):
    db = Database()

    sql = f"""
    SELECT TOP 1 ID
    FROM LEADS
    WHERE LEAD_ID = '{clean_text(lead_id, 100)}'
      AND IS_PROCESSED = 1
    """

    status, rows = db.db_query(sql)

    if status and rows:
        return True

    return False


# ------------------------------------------------
# MARK LEAD AS PROCESSED
# ------------------------------------------------
def mark_lead_processed(lead_id):
    db = Database()

    sql = f"""
    UPDATE LEADS
    SET
        IS_PROCESSED = 1,
        MODIFIED_DATE = GETDATE()
    WHERE LEAD_ID = '{clean_text(lead_id, 100)}'
    """

    db.db_update(sql)


# ------------------------------------------------
# INSERT CUSTOMER FROM LEAD
# EXACT COPY FROM LEADS
# ------------------------------------------------
def insert_customer(row):
    try:
        db = Database()

        # --------------------------------
        # LEADS TABLE FIELD MAPPING
        # --------------------------------
        lead_id = clean_text(row[1], 100)
        name = clean_text(row[13], 150)
        phone = clean_text(row[14], 30)
        email = clean_text(row[15], 150)
        city = clean_text(row[16], 100)
        month = clean_text(row[17], 100)

        # EXACT SAVE
        contact_mode = get_exact_contact_mode(row[18])

        pax = parse_pax(row[19])
        platform = clean_text(row[12], 50)

        # --------------------------------
        # BRANCH + CRE ASSIGNMENT
        # --------------------------------
        branch_id = get_branch_id(city)
        cre_id = get_next_cre_by_branch(branch_id)

        log(f"👤 Lead → {name} | {phone}")
        log(f"📞 Contact Mode (Exact) → {contact_mode}")
        log(f"🌐 Platform → {platform}")
        log(f"📍 Branch → {branch_id} | 🎯 CRE → {cre_id}")

        # --------------------------------
        # VALIDATION
        # --------------------------------
        if not phone and not email:
            return

        if is_lead_processed(lead_id):
            return

        if customer_exists(phone, email):
            log(f"⚠ Duplicate skipped → {phone or email}")
            mark_lead_processed(lead_id)
            return

        # --------------------------------
        # INSERT INTO CRM_CUSTOMERS
        # --------------------------------
        sql = f"""
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
            '{name}',
            '{phone}',
            '{email}',
            '{cre_id if cre_id else ''}',
            '',
            '{month}',
            {pax},
            '{contact_mode}',
            'Pending',
            '',
            GETDATE(),
            GETDATE(),
            '{branch_id}',
            '{platform}'
        )
        """

        status, result = db.db_update(sql)

        if status:
            log(f"✅ CRM Customer Created → {name}")
            mark_lead_processed(lead_id)
        else:
            log(f"❌ Insert error → {result}")

    except Exception as e:
        log(f"❌ Insert exception → {e}")


# ------------------------------------------------
# REPAIR OLD CUSTOMER DATA
# Exact copy from LEADS
# ------------------------------------------------
def repair_old_customer_data():
    db = Database()

    # --------------------------------
    # FIX CONTACT_MODE + STATUS + PLATFORM
    # --------------------------------
    sql1 = """
    UPDATE C
    SET
        C.CONTACT_MODE = ISNULL(L.PREFERRED_MODE_OF_CONTACT, C.CONTACT_MODE),
        C.STATUS = CASE
            WHEN C.STATUS IS NULL OR LTRIM(RTRIM(C.STATUS)) = '' THEN 'Pending'
            ELSE C.STATUS
        END,
        C.PLATFORM = CASE
            WHEN C.PLATFORM IS NULL OR LTRIM(RTRIM(C.PLATFORM)) = '' THEN ISNULL(L.PLATFORM, '')
            ELSE C.PLATFORM
        END,
        C.MODIFIED_DATE = GETDATE()
    FROM CRM_CUSTOMERS C
    INNER JOIN LEADS L
        ON (
            (ISNULL(C.PHONE_NUMBER, '') <> '' AND C.PHONE_NUMBER = L.PHONE_NUMBER)
            OR
            (ISNULL(C.EMAIL, '') <> '' AND C.EMAIL = L.EMAIL)
        )
    WHERE
        (
            C.CONTACT_MODE IS NULL
            OR LTRIM(RTRIM(C.CONTACT_MODE)) = ''
            OR C.STATUS IS NULL
            OR LTRIM(RTRIM(C.STATUS)) = ''
            OR C.PLATFORM IS NULL
            OR LTRIM(RTRIM(C.PLATFORM)) = ''
        )
    """

    # --------------------------------
    # FIX CRE_ID if missing
    # --------------------------------
    sql2 = """
    ;WITH CRE_LIST AS (
        SELECT
            S.BRANCH_ID,
            S.EMPID,
            ROW_NUMBER() OVER (
                PARTITION BY S.BRANCH_ID
                ORDER BY S.EMPID
            ) AS RN
        FROM SW_USER S
        WHERE UPPER(LTRIM(RTRIM(ISNULL(S.ROLES, '')))) = 'CRE'
          AND (S.ISDISABLED IS NULL OR S.ISDISABLED = 0)
    )
    UPDATE C
    SET
        C.CRE_ID = CL.EMPID,
        C.MODIFIED_DATE = GETDATE()
    FROM CRM_CUSTOMERS C
    INNER JOIN CRE_LIST CL
        ON C.BRANCH_ID = CL.BRANCH_ID
       AND CL.RN = 1
    WHERE C.CRE_ID IS NULL OR C.CRE_ID = ''
    """

    status1, result1 = db.db_update(sql1)
    status2, result2 = db.db_update(sql2)

    if status1 and status2:
        log("⚡ CONTACT_MODE + STATUS + PLATFORM + CRE_ID repair completed")
    else:
        log(f"❌ Repair failed → {result1} | {result2}")


# ------------------------------------------------
# PROCESS NEW LEADS
# ------------------------------------------------
def process_leads():
    db = Database()

    sql = """
    SELECT *
    FROM LEADS
    WHERE ISNULL(IS_PROCESSED, 0) = 0
      AND CREATED_DATE >= DATEADD(DAY, -30, GETDATE())
    ORDER BY ID ASC
    """

    status, rows = db.db_query(sql)

    if not status or not rows:
        return

    for row in rows:
        insert_customer(row)


# ------------------------------------------------
# START CRM CUSTOMER SYNC
# ------------------------------------------------
def start_crm_customer_sync():
    log("🚀 CRM Customer Sync Started")

    while True:
        try:
            process_leads()
            repair_old_customer_data()
        except Exception as e:
            log(f"❌ CRM Sync Error → {e}")

        time.sleep(10)