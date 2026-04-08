from flask import Flask, request, render_template_string
import random, json, os, time, threading
import redis
import psycopg2

app = Flask(__name__)

# ===== CONFIG =====
PENDING_TTL = 600
LOCK_TTL = 600
MAX_WORKERS_PER_STORE = 2
BIG_STORE_THRESHOLD = 1200
SMALL_STORE_THRESHOLD = 400

r = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)

DB_URL = os.environ.get("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

# ===== SPLIT =====
def split_replen_and_other(orders):
    replen, other = [], []
    for o in orders:
        if "replen" in o["susr3"].lower():
            replen.append(o)
        else:
            other.append(o)
    return replen, other

# ===== LOAD ORDERS FROM DB =====
def load_orders(force_refresh=False):
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

# ===== PRIORITY =====
def get_priority(store):
    if store.startswith("25"):
        return 1
    if store.startswith(("412", "413")):
        return 2
    if store.startswith(("41", "42")):
        return 3
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
    SECOND = False

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
        assigned = list(store_orders)

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

    if not assigned:
        return [], False, False

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

    # LOG QUEUE
    r.rpush("log_queue", json.dumps({
        "user": user,
        "orders": orders
    }))

# ===== FINISH =====
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

# ===== LOG WORKER =====
def log_worker():
    while True:
        item = r.lpop("log_queue")
        if not item:
            time.sleep(1)
            continue

        data = json.loads(item)

        try:
            conn = get_conn()
            cur = conn.cursor()

            for o in data["orders"]:
                cur.execute(
                    "INSERT INTO logs (user_id, order_id, store, ref) VALUES (%s,%s,%s,%s)",
                    (data["user"], o["order"], o["store"], o["ref"])
                )

            conn.commit()
            conn.close()

        except Exception as e:
            print("LOG ERROR:", e)
            r.rpush("log_queue", item)
            time.sleep(2)

threading.Thread(target=log_worker, daemon=True).start()

# ===== HTML =====
HTML = """
<h2>📦 Dystrybucja zamówień</h2>

<form method="post">
    <input name="user" placeholder="Wpisz ID" required>
    <button name="action" value="get">Pobierz zamówienia</button>
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
<h3>📊 Statystyка</h3>
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

# ===== STATS =====
def get_stats():
    stats = {}
    for k in r.scan_iter("assigned:*"):
        user = k.split(":")[1]
        stats[user] = r.llen(k)
    return stats

app.run(host="0.0.0.0", port=5000)
