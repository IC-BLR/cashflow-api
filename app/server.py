"""
FastAPI application setup and configuration.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from typing import Optional

from app.endpoints.routes import APIRoutes
from app.services import APIServices

# ============================================================
# Paths & Environment
# ============================================================

ROOT_DIR = Path(__file__).parent.parent  # Go up one level from app/ to backend/
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DUCKDB_PATH = "payment_allocation.duckdb"
LOG_FILE = DATA_DIR / "app.log"

load_dotenv(ROOT_DIR / ".env")

# ============================================================
# Logging Configuration
# ============================================================

def setup_logging():
    """Configure logging with file and console handlers."""
    # Clear any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Log format for file (detailed)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Log format for console (simpler)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # File handler with rotation (10MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)
logger.info(f"Logging initialized. Log file: {LOG_FILE}")

# ============================================================
# App & Router
# ============================================================

app = FastAPI(title="Cashflow Trend Analysis API")

# Create API router with /api prefix
api_router = APIRouter(prefix="/api")

# ============================================================
# DuckDB (lifecycle managed)
# ============================================================

duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_duckdb():
    logger.debug("Acquiring DuckDB connection")
    """Get DuckDB connection."""
    if duckdb_conn is None:
        raise RuntimeError("DuckDB not initialized")
    return duckdb_conn


# ============================================================
# DuckDB Initialization
# ============================================================

def init_duckdb():
    logger.info("Initializing DuckDB tables")
    """Initialize DuckDB tables."""
    conn = get_duckdb()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            contact_email VARCHAR,
            total_revenue DOUBLE,
            total_expenses DOUBLE,
            status VARCHAR,
            created_at TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id VARCHAR PRIMARY KEY,
            partner_id VARCHAR,
            partner_name VARCHAR,
            invoice_number VARCHAR,
            amount DOUBLE,
            type VARCHAR,
            status VARCHAR,
            due_date DATE,
            paid_date DATE,
            category VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cashflow_entries (
            id VARCHAR PRIMARY KEY,
            date DATE,
            inflow DOUBLE,
            outflow DOUBLE,
            net_flow DOUBLE,
            running_balance DOUBLE,
            category VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS partner_llm_insights (
            partner_code VARCHAR PRIMARY KEY,
            insights_json TEXT,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        )
    """)


# ============================================================
# FastAPI Lifecycle
# ============================================================

@app.on_event("startup")
async def startup():
    """Initialize application on startup."""
    global duckdb_conn
    duckdb_conn = duckdb.connect(str(DUCKDB_PATH))
    init_duckdb()
    logger.info(f"DuckDB initialized at {DUCKDB_PATH}")
    
    # Initialize services and routes
    services = APIServices(get_duckdb)
    routes = APIRoutes(api_router, services)
    app.include_router(api_router)
    logger.info("API routes initialized")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    global duckdb_conn
    if duckdb_conn:
        duckdb_conn.close()
    logger.info("Shutdown complete")


# ============================================================
# Middleware
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)