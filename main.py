import threading
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from crm import router, start_polling, import_all, init_engine
from crm_customer import start_crm_customer_sync


# ==========================================
# SIMPLE LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("main")


# ==========================================
# BACKGROUND STARTUP
# ==========================================
def delayed_start():
    try:
        logger.info("🚀 Starting background services...")
        time.sleep(2)

        # 1) Init engine first
        init_engine()
        logger.info("✅ CRM Engine initialized")

        # 2) Historical import in background
        threading.Thread(target=import_all, daemon=True).start()
        logger.info("📥 Historical import started")

        # 3) Polling backup in background
        start_polling()
        logger.info("🔄 Polling backup started")

        # 4) CRM customer sync in background
        threading.Thread(target=start_crm_customer_sync, daemon=True).start()
        logger.info("👥 CRM Customer Sync started")

    except Exception as e:
        logger.error(f"❌ Startup error: {e}")


# ==========================================
# FASTAPI LIFESPAN
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 CRM Lead Engine starting...")

    threading.Thread(target=delayed_start, daemon=True).start()

    yield

    logger.info("🛑 CRM Lead Engine stopped")


# ==========================================
# APP
# ==========================================
app = FastAPI(
    title="CRM Lead Engine",
    version="1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


# ==========================================
# ROOT
# ==========================================
@app.get("/")
def root():
    return {
        "status": "running",
        "service": "CRM Lead Engine"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }