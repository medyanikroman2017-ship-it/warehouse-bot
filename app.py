from flask import Flask, request, render_template_string
import random, json, os, time, threading
import redis
import psycopg2
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# ===== CONFIG =====
PENDING_TTL = 600
LOCK_TTL = 600
BIG_STORE_THRESHOLD = 1200
SMALL_STORE_THRESHOLD = 400

r = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)
DB_URL = os.environ.get("DATABASE_URL")

# ===== DB =====
def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

# ===== GOOGLE SHEETS =====
def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(creds)
    return client.open_by_key("1dtSO224vSpxaR5Jm3wNQ09SiMjSjLGkgL1C4lRfg7YM")

# ===== LOG WORKER (НАДЕЖНЫЙ) =====
def log_worker():
    sheet = connect_sheet().worksheet("log")

    BATCH_SIZE = 20
    buffer = []
    processing_items = []

    while True:
        item = r.rpoplpush("log_queue", "log_processing")

        if item:
            data = json.loads(item)

            # защита от дублей
            if r.sismember("logged_ids", data.get("id")):
                r.lrem("log_processing", 1, item)
                continue

            for o in data["orders"]:
                buffer.append([
                    data["user"],
                    o["order"],
                    o["store"],
                    o["ref"],
                    time.strftime("%Y-%m-%d %H:%M:%S")
                ])

            processing_items.append(item)

        if len(buffer) >= BATCH_SIZE:
            try:
                sheet.append_rows(buffer)

                # помечаем как записанные
                for item in processing_items:
                    data = json.loads(item)
                    r.sadd("logged_ids", data.get("id"))
                    r.lrem("log_processing", 1, item)

                buffer = []
                processing_items = []

            except Exception as e:
                print("LOG ERROR:", e)
                time.sleep(2)

        time.sleep(1)

threading.Thread(target=log_worker, daemon=True).start()

# ===== UPLOAD =====
def upload_orders(file):
    df = pd.read_excel(file)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM orders")

    for _, row in df.iterrows():
        try:
            cur.execute(
                """
                INSERT INTO orders (order_id, store, qty, susr3, ref)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    str(row["ORDERKEY"]).zfill(10),
                    str(row["CONSIGNEEKEY"]),
                    int(row["TOTALQTY"]),
                    str(row["SUSR3"] or ""),
                    str(row["REFERENCENUM"] or "")
                )
            )
        except Exception as e:
            print("UPLOAD ERROR:", e)

    conn.commit()
    conn.close()

# ===== LOAD =====
def load_orders():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT order_id, store, qty, susr3, ref FROM orders")
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "order": r[0],
            "store": r[1],
            "qty": r[2],
            "susr3": r[3] or "",
            "ref": r[4] or "",
        }
        for r in rows
    ]

# ===== SPLIT =====
def split_replen_and_other(orders):
    replen, other = [], []
    for o in orders:
        if "replen" in o["susr3"].lower():
            replen.append(o)
        else:
            other.append(o)
    return replen, other

# ===== PRIORITY =====
def get_priority(store):
    if store.startswith("25"):
        return 1
    if store.startswith(("412", "413")):
        return 2
    if store.startswith(("41", "42")):
        return 3
    return 4

# ===== LOCK =====
LOCK_SCRIPT = r.register_script("""
local user = ARGV[1]
local ttl = tonumber(ARGV[2])

for i,key in ipairs(KEYS) do
    if redis.call("EXISTS", key) == 1 then
        return 0
    end
end

for i,key in ipairs(KEYS) do
    redis.call("SET", key, user, "EX", ttl)
    redis.call("SADD", "locked_orders", string.sub(key,6))
end

return 1
""")

def lock_orders(user, orders):
    keys = [f"lock:{o['order']}" for o in orders]
    return LOCK_SCRIPT(keys=keys, args=[user, LOCK_TTL]) == 1

def get_store_workers():
    return {k: int(v) for k, v in r.hgetall("store_workers").items()}

# ===== ASSIGN =====
def assign_orders(user):
    pending = r.get(f"pending:{user}")
    if pending:
        return json.loads(pending), False, False

    orders = load_orders()

    locked = r.smembers("locked_orders")
    assigned_global = r.smembers("assigned_orders")

    available = [
        o for o in orders
        if o["order"] not in locked
        and o["order"] not in assigned_global
    ]

    if not available:
        return [], False, False

    stores, store_qty = {}, {}

    for o in available:
        s = o["store"]
        stores.setdefault(s, []).append(o)
        store_qty[s] = store_qty.get(s, 0) + o["qty"]

    workers = get_store_workers()

    valid = []
    for s in sorted(stores, key=get_priority):
        w = workers.get(s, 0)
        qty = store_qty[s]

        if (qty >= BIG_STORE_THRESHOLD and w < 2) or (
            qty < BIG_STORE_THRESHOLD and w == 0
        ):
            valid.append(s)

    if not valid:
        return [], False, False

    best_p = get_priority(valid[0])
    chosen = random.choice([s for s in valid if get_priority(s) == best_p])

    store_orders = stores[chosen]
    total_qty = store_qty[chosen]

    BIG = total_qty >= BIG_STORE_THRESHOLD

    if BIG:
        replen, other = split_replen_and_other(store_orders)
        current_workers = workers.get(chosen, 0)

        if current_workers == 0:
            assigned = replen if replen else store_orders
        elif current_workers == 1:
            assigned = other if other else replen
        else:
            return [], False, False
    else:
        assigned = store_orders

    if not assigned:
        return [], False, False

    if not lock_orders(user, assigned):
        return [], False, False

    r.setex(f"pending:{user}", PENDING_TTL, json.dumps(assigned))

    return assigned, BIG, False

# ===== CONFIRM =====
def confirm_orders(user):
    raw = r.get(f"pending:{user}")
    if not raw:
        return

    orders = json.loads(raw)

    pipe = r.pipeline()

    for o in orders:
        oid = o["order"]
        pipe.hincrby("store_workers", o["store"], 1)
        pipe.persist(f"lock:{oid}")
        pipe.sadd("assigned_orders", oid)

    pipe.delete(f"pending:{user}")
    pipe.execute()

    # УНИКАЛЬНЫЙ ID события
    log_event = {
        "id": f"{user}_{time.time()}",
        "user": user,
        "orders": orders
    }

    r.rpush("log_queue", json.dumps(log_event))

# ===== HTML =====
HTML = """
<h2>📦 Dystrybucja zamówień</h2>

<form method="post">
    <input name="user" placeholder="Wpisz ID" required>
    <button name="action" value="get">Pobierz zamówienia</button>
</form>

{% if user == "admin" %}
<form method="post" enctype="multipart/form-data">
    <input type="hidden" name="user" value="{{user}}">
    <input type="file" name="file" required>
    <button name="action" value="upload">📤 Upload Excel</button>
</form>
{% endif %}

{% if orders %}
<h3>👤 {{user}}</h3>

<form method="post">
    <input type="hidden" name="user" value="{{user}}">
    <button name="action" value="confirm">✅ Potwierdź odbiór</button>
</form>

{% for o in orders %}
<div>{{o.order}} | {{o.store}} | {{o.qty}} | {{o.susr3}}</div>
{% endfor %}
{% endif %}

{% if user and not orders %}
<div style="color:gray; margin-top:20px;">
    ❌ Brak dostępnych zamówień do pobrania
</div>
{% endif %}
"""

# ===== ROUTE =====
@app.route("/", methods=["GET", "POST"])
def index():
    user = request.form.get("user")
    action = request.form.get("action")
    file = request.files.get("file")

    orders = []

    if user:
        if action == "upload" and user == "admin" and file:
            upload_orders(file)

        elif action == "confirm":
            confirm_orders(user)

        else:
            orders, _, _ = assign_orders(user)

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
    )

app.run(host="0.0.0.0", port=5000)
