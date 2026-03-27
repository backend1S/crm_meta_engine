import threading
import logging
import subprocess
import time
import requests
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from crm import router, start_polling, import_all, init_engine
from crm_customer import start_crm_customer_sync


# ------------------------------------------------
# LOGGING
# ------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ------------------------------------------------
# FASTAPI APP
# ------------------------------------------------
app = FastAPI(
    title="CRM Lead Engine",
    version="1.0"
)


# ------------------------------------------------
# CORS
# ------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------
# ROUTERS
# ------------------------------------------------
app.include_router(router)


# ------------------------------------------------
# ROOT API
# ------------------------------------------------
@app.get("/")
def root():
    return {"status": "CRM Lead Engine Running"}


# ------------------------------------------------
# START NGROK
# ------------------------------------------------
def start_ngrok():
    if os.getenv("ENABLE_NGROK", "0") != "1":
        logger.info("ℹ️ Ngrok disabled")
        return

    try:
        logger.info("🚀 Starting ngrok...")

        if os.name == "nt":
            os.system("taskkill /f /im ngrok.exe >nul 2>&1")

        subprocess.Popen(
            ["ngrok", "http", "8000"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        time.sleep(5)

        res = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=10)
        tunnels = res.json()
        public_url = tunnels["tunnels"][0]["public_url"]

        logger.info(f"🌍 Ngrok URL → {public_url}")
        logger.info(f"🔗 Webhook URL → {public_url}/crm/webhook")

    except Exception as e:
        logger.error(f"❌ Ngrok error → {e}")


# ------------------------------------------------
# RUN LEAD AUTOMATION
# ------------------------------------------------
def run_lead_engine():
    logger.info("🚀 Starting Facebook Lead Engine")

    init_engine()

    import_thread = threading.Thread(target=import_all, daemon=True)
    import_thread.start()

    start_polling()


# ------------------------------------------------
# RUN CRM CUSTOMER SYNC
# ------------------------------------------------
def run_customer_sync():
    logger.info("🚀 Starting CRM Customer Sync")
    start_crm_customer_sync()


# ------------------------------------------------
# STARTUP EVENT
# ------------------------------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting Lead Engine Automation")

    threading.Thread(target=start_ngrok, daemon=True).start()
    threading.Thread(target=run_lead_engine, daemon=True).start()
    threading.Thread(target=run_customer_sync, daemon=True).start()