from flask import Flask, request, render_template_string
import random, json, os, time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import redis

app = Flask(__name__)

# ===== CONFIG =====
PENDING_TTL = 600
LOCK_TTL = 600
MAX_WORKERS_PER_STORE = 2
BIG_STORE_THRESHOLD = 1200
SMALL_STORE_THRESHOLD = 400

ORDERS_CACHE_KEY = "orders_cache"
ORDERS_TTL = 30

r = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)

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

# ===== ORDERS CACHE =====
def load_orders(force_refresh=False):
    if force_refresh:
        r.delete(ORDERS_CACHE_KEY)

    cached = r.get(ORDERS_CACHE_KEY)

    if cached and not force_refresh:
        print("📦 LOAD FROM CACHE")
        return json.loads(cached)

    print("📡 LOAD FROM GOOGLE SHEETS")

    sheet = connect_sheet().worksheet("orders")
    data = sheet.get_all_records()

    orders = [
        {
            "order": str(row["ORDERKEY"]).zfill(10),
            "store": str(row["CONSIGNEEKEY"]),
            "qty": int(row["TOTALQTY"]) if row["TOTALQTY"] else 0,
            "susr3": str(row["SUSR3"] or ""),
            "ref": str(row["REFERENCENUM"] or ""),
        }
        for row in data
    ]

    r.setex(ORDERS_CACHE_KEY, ORDERS_TTL, json.dumps(orders))

    return orders

# ===== PRIORITY =====
def get_priority(store):
    if store.startswith("25"):
        return 1
    if store.startswith(("412", "413")):
        return 2
    if store.startswith(("41", "42")):
        return 3
    if store.startswith("496"):
        return 4
    return 4

# ===== LUA LOCK =====
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

# ===== STORE WORKERS =====
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

        if (qty >= BIG_STORE_THRESHOLD and w < MAX_WORKERS_PER_STORE) or (
            qty < BIG_STORE_THRESHOLD and w == 0
        ):
            valid.append(s)

    if not valid:
        return [], False, False

    best_p = get_priority(valid[0])
    chosen = random.choice([s for s in valid if get_priority(s) == best_p])

    assigned = stores[chosen]
    total_qty = store_qty[chosen]

    BIG = total_qty >= BIG_STORE_THRESHOLD
    SECOND = False

    if total_qty <= SMALL_STORE_THRESHOLD:
        for s in stores:
            if (
                s != chosen
                and store_qty[s] <= SMALL_STORE_THRESHOLD
                and workers.get(s, 0) == 0
            ):
                assigned += stores[s]
                SECOND = True
                break

    if not lock_orders(user, assigned):
        return [], False, False

    r.setex(f"pending:{user}", PENDING_TTL, json.dumps(assigned))

    return assigned, BIG, SECOND

# ===== CONFIRM =====
def confirm_orders(user):
    raw = r.get(f"pending:{user}")
    if not raw:
        return

    orders = json.loads(raw)

    pipe = r.pipeline()

    for o in orders:
        oid = o["order"]

        pipe.rpush(f"assigned:{user}", oid)
        pipe.hincrby("store_workers", o["store"], 1)
        pipe.persist(f"lock:{oid}")
        pipe.sadd("assigned_orders", oid)

    pipe.delete(f"pending:{user}")
    pipe.execute()

    r.set(f"user_orders:{user}", json.dumps(orders))

    log_to_sheet(user, orders)

# ===== FINISH ONE =====
def finish_one_order(user, order_id):
    raw = r.get(f"user_orders:{user}")
    if not raw:
        return

    orders = json.loads(raw)
    order_obj = next((o for o in orders if o["order"] == order_id), None)
    if not order_obj:
        return

    store = order_obj["store"]

    pipe = r.pipeline()
    pipe.delete(f"lock:{order_id}")
    pipe.srem("assigned_orders", order_id)
    pipe.lrem(f"assigned:{user}", 0, order_id)
    pipe.hincrby("store_workers", store, -1)
    pipe.srem("locked_orders", order_id)
    pipe.execute()

    new_orders = [o for o in orders if o["order"] != order_id]

    if new_orders:
        r.set(f"user_orders:{user}", json.dumps(new_orders))
    else:
        r.delete(f"user_orders:{user}")

# ===== LOG =====
def log_to_sheet(user, orders):
    sheet = connect_sheet().worksheet("log")
    for o in orders:
        sheet.append_row([
            user, o["order"], o["store"], o["ref"],
            time.strftime("%Y-%m-%d %H:%M:%S")
        ])

# ===== STATS =====
def get_stats():
    stats = {}
    for k in r.scan_iter("assigned:*"):
        user = k.split(":")[1]
        stats[user] = r.llen(k)
    return stats

# ===== HTML =====
HTML = """
<h2>📦 Dystrybucja zamówień</h2>

<form method="post">
    <input name="user" placeholder="Wpisz ID" required>
    <button name="action" value="get">Pobierz zamówienia</button>
    <button name="action" value="refresh">🔄 Refresh</button>
</form>

{% if orders %}
<h3>👤 {{user}}</h3>

<form method="post">
    <input type="hidden" name="user" value="{{user}}">
    <button name="action" value="confirm">✅ Potwierdź odbióр</button>
</form>

{% set grouped = {} %}
{% for o in orders %}
    {% if o.store not in grouped %}
        {% set _ = grouped.update({o.store: []}) %}
    {% endif %}
    {% set _ = grouped[o.store].append(o) %}
{% endfor %}

{% for store, items in grouped.items() %}
<h3>🏬 {{store}}</h3>
<ul>
{% for i in items %}
<li>
    {{i.order}} ({{i.susr3}}) — {{i.qty}}

    <form method="post" style="display:inline;">
        <input type="hidden" name="user" value="{{user}}">
        <input type="hidden" name="order_id" value="{{i.order}}">
        <button name="action" value="finish_one">🏁</button>
    </form>
</li>
{% endfor %}
</ul>
{% endfor %}
{% endif %}

<hr>
<h3>📊 Statystyka</h3>
{% for u, c in stats.items() %}
<div>{{u}} → {{c}}</div>
{% endfor %}
"""

# ===== ROUTE =====
@app.route("/", methods=["GET", "POST"])
def index():
    user = request.form.get("user")
    action = request.form.get("action")
    order_id = request.form.get("order_id")

    orders, big, second = [], False, False

    if user:
        if action == "confirm":
            confirm_orders(user)

        elif action == "finish_one" and order_id:
            finish_one_order(user, order_id)

        elif action == "refresh":
            load_orders(force_refresh=True)
            orders, big, second = assign_orders(user)

        else:
            orders, big, second = assign_orders(user)

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
        stats=get_stats(),
        big=big,
        second=second
    )

app.run(host="0.0.0.0", port=5000)
