#!/usr/bin/env python3
#
# ccwg-db-server.py — CommonCrawl WebGraph Rank Lookup Server (Python + FastAPI)
#
# SETUP & RUN (one time):
#   python3 -m venv venv
#   source venv/bin/activate
#   pip install fastapi uvicorn
#   python3 ccwg-db-server.py
#
# RUN (after initial setup):
#   source venv/bin/activate
#   python3 ccwg-db-server.py
#
# Or with uvicorn directly (venv must be active):
#   uvicorn ccwg-db-server:app --host 127.0.0.1 --port 8999 --workers 16
#
# REQUIRES:
#   Python 3.8+
#   fastapi and uvicorn (installed via pip inside the venv above)
#   sqlite3 and json are stdlib — no additional packages needed
#
# DESCRIPTION:
#   Single-file FastAPI HTTP server for CommonCrawl WebGraph rank lookups.
#   Accepts plain-text POST bodies (one hostname per line, max 10), looks
#   up each hostname in two sets of sharded SQLite databases (host + domain),
#   and returns structured JSON with historical rank data.
#
#   Based on ApiServer.php but without rate limiting.
#
# ARCHITECTURE:
#   When run via `python3 ccwg-db-server.py`, Uvicorn is started with
#   WORKER_NUM worker processes. Each worker opens all 64 SQLite databases
#   (32 domain shards + 32 host shards) as read-only handles on startup.
#   Uvicorn dispatches incoming connections across workers automatically.
#
# DATA FLOW:
#   1. Client POSTs plain text to / (one hostname per line, max 10).
#   2. Each hostname is normalized: lowercase, strip trailing dot, reverse
#      dot notation (e.g. "Google.COM" -> "com.google").
#   3. The reversed key is DJB2-hashed to select a shard (0-31).
#   4. Both the host DB and domain DB for that shard are queried.
#   5. Results are returned as JSON keyed by the original submitted hostname.
#

import os
import sqlite3
import datetime

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import uvicorn


# --- Network ---

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8999


# --- Workers ---

WORKER_NUM = 16


# --- Database paths ---

DOMAIN_DB_DIR = "/home/oa_adm/cc-webgraph/data-merged-domains"
HOST_DB_DIR   = "/home/oa_adm/cc-webgraph/data-merged-hosts"


# --- Sharding ---

SHARD_COUNT    = 32
SHARD_BIT_MASK = 0x1F
DJB2_SEED      = 5381
DJB2_U32_MASK  = 0xFFFFFFFF


# --- Request limits ---

MAX_HOSTS_PER_REQUEST = 10


# --- Access logging ---

LOG_DIR              = "logs"
LOG_FLUSH_SIZE_BYTES = 6144
LOG_FLUSH_INTERVAL_S = 300


# --- SQLite tuning ---

SQLITE_CACHE_SIZE_KB = 65536
SQLITE_MMAP_SIZE     = 268435456


#------------------------------------------------------------------------------
def djb2_hash(s):
    h = DJB2_SEED
    for c in s:
        h = ((h << 5) + h + ord(c)) & DJB2_U32_MASK
    return h


#------------------------------------------------------------------------------
def normalize(hostname):
    hostname = hostname.strip().lower().rstrip(".")
    parts = hostname.split(".")
    parts.reverse()
    return ".".join(parts)


#------------------------------------------------------------------------------
def parse_tsv_data(raw_data):
    entries = []
    for line in raw_data.split("\n"):
        line = line.strip()
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        entry = {
            "year_month":  fields[0],
            "hc_pos":      int(fields[1]),
            "hc_val":      int(fields[2]),
            "pr_pos":      int(fields[3]),
            "pr_val":      int(fields[4]),
            "hc_val_norm": int(fields[5]),
            "pr_val_norm": int(fields[6]),
        }
        if len(fields) >= 8:
            entry["n_hosts"] = int(fields[7])
        entries.append(entry)
    return entries


################################################################################
class DbPool:
    def __init__(self, domain_db_dir, host_db_dir):
        self.domain_dbs = [None] * SHARD_COUNT
        self.host_dbs   = [None] * SHARD_COUNT
        self.domain_db_dir = domain_db_dir
        self.host_db_dir   = host_db_dir

    #--------------------------------------------------------------------------
    def open_all(self):
        for i in range(SHARD_COUNT):
            self.domain_dbs[i] = self._open_shard(self.domain_db_dir, "domain", i)
            self.host_dbs[i]   = self._open_shard(self.host_db_dir, "host", i)

    #--------------------------------------------------------------------------
    def lookup_domain(self, shard, reversed_host):
        return self._query(self.domain_dbs[shard], reversed_host)

    #--------------------------------------------------------------------------
    def lookup_host(self, shard, reversed_host):
        return self._query(self.host_dbs[shard], reversed_host)

    #--------------------------------------------------------------------------
    def close_all(self):
        for i in range(SHARD_COUNT):
            if self.domain_dbs[i] is not None:
                self.domain_dbs[i].close()
                self.domain_dbs[i] = None
            if self.host_dbs[i] is not None:
                self.host_dbs[i].close()
                self.host_dbs[i] = None

    #--------------------------------------------------------------------------
    def _open_shard(self, db_dir, db_type, shard):
        path = os.path.join(db_dir, "%s.%d.db" % (db_type, shard))
        if not os.path.isfile(path):
            return None
        conn = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
        try:
            conn.execute("PRAGMA cache_size = -%d" % SQLITE_CACHE_SIZE_KB)
            conn.execute("PRAGMA mmap_size = %d" % SQLITE_MMAP_SIZE)
        except Exception:
            pass
        return conn

    #--------------------------------------------------------------------------
    def _query(self, conn, reversed_host):
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute(
            "SELECT data FROM host_data WHERE host = ? LIMIT 1",
            (reversed_host,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]


################################################################################
class AccessLogger:
    def __init__(self, worker_pid):
        os.makedirs(LOG_DIR, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.file_path = os.path.join(LOG_DIR, "CCWG-DB-%s-%d.log" % (ts, worker_pid))
        self.buffer = ""

    #--------------------------------------------------------------------------
    def append(self, ip, hostnames):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hosts_str = " ".join(hostnames)
        self.buffer += "%s\t%s\t%s\n" % (ts, ip, hosts_str)
        if len(self.buffer) >= LOG_FLUSH_SIZE_BYTES:
            self.flush()

    #--------------------------------------------------------------------------
    def flush(self):
        if not self.buffer:
            return
        with open(self.file_path, "a") as f:
            f.write(self.buffer)
        self.buffer = ""


################################################################################
# Global state — initialized per worker process
################################################################################

db_pool = None
logger  = None

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


#------------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    global db_pool, logger
    db_pool = DbPool(DOMAIN_DB_DIR, HOST_DB_DIR)
    db_pool.open_all()
    logger = AccessLogger(os.getpid())
    print("Worker %d started, databases opened." % os.getpid())


#------------------------------------------------------------------------------
@app.on_event("shutdown")
def shutdown_event():
    global db_pool, logger
    if logger is not None:
        logger.flush()
    if db_pool is not None:
        db_pool.close_all()
    print("Worker %d stopped, databases closed." % os.getpid())


#------------------------------------------------------------------------------
@app.post("/")
async def lookup(request: Request):
    body = await request.body()
    body_str = body.decode("utf-8", errors="replace")

    if not body_str.strip():
        return JSONResponse(
            status_code=400,
            content={
                "error": "Request must contain 1-%d hostnames, one per line" % MAX_HOSTS_PER_REQUEST,
                "max_per_request": MAX_HOSTS_PER_REQUEST,
            },
        )

    hostnames = [line.strip() for line in body_str.split("\n") if line.strip()]
    host_count = len(hostnames)

    if host_count < 1 or host_count > MAX_HOSTS_PER_REQUEST:
        return JSONResponse(
            status_code=400,
            content={
                "error": "Request must contain 1-%d hostnames, one per line" % MAX_HOSTS_PER_REQUEST,
                "max_per_request": MAX_HOSTS_PER_REQUEST,
            },
        )

    ip = request.headers.get("x-real-ip")
    if ip is None:
        ip = request.headers.get("x-forwarded-for", request.client.host)
    if "," in ip:
        ip = ip.split(",")[0].strip()

    if logger is not None:
        logger.append(ip, hostnames)

    results = {}
    for hostname in hostnames:
        reversed_host = normalize(hostname)
        h = djb2_hash(reversed_host)
        shard = h & SHARD_BIT_MASK

        host_data   = []
        domain_data = []

        raw_host = db_pool.lookup_host(shard, reversed_host)
        if raw_host is not None:
            host_data = parse_tsv_data(raw_host)

        raw_domain = db_pool.lookup_domain(shard, reversed_host)
        if raw_domain is not None:
            domain_data = parse_tsv_data(raw_domain)

        results[hostname] = {
            "host":   host_data,
            "domain": domain_data,
        }

    return {"results": results}


#------------------------------------------------------------------------------
@app.api_route("/", methods=["GET", "PUT", "DELETE", "PATCH"])
async def method_not_allowed():
    return JSONResponse(
        status_code=405,
        content={"error": "Method not allowed. Use POST."},
    )


#------------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== CommonCrawl WebGraph DB Server (Python/FastAPI) ===")
    print("Listening on %s:%d" % (LISTEN_HOST, LISTEN_PORT))
    print("Workers: %d" % WORKER_NUM)
    print("Domain DB dir: %s" % DOMAIN_DB_DIR)
    print("Host DB dir:   %s" % HOST_DB_DIR)
    print("Max per request: %d" % MAX_HOSTS_PER_REQUEST)
    print()

    uvicorn.run(
        "ccwg-db-server:app",
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        workers=WORKER_NUM,
        log_level="warning",
    )
