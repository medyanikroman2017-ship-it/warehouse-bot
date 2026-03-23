from flask import Flask, request, render_template_string
import random, json, os, pandas as pd, time

app = Flask(__name__)

DATA_FILE = "assigned.json"
PENDING_FILE = "pending.json"
LOG_FILE = "log.xlsx"

TTL = 1800
MAX_WORKERS_PER_STORE = 2
BIG_STORE_THRESHOLD = 1200
SMALL_STORE_THRESHOLD = 300

# ===== LOAD EXCEL =====
def load_orders():
    df = pd.read_excel("orders.xlsx")
    orders = []

    for _, row in df.iterrows():
        if pd.isna(row["ORDERKEY"]) or pd.isna(row["CONSIGNEEKEY"]):
            continue

        orders.append({
            "order": str(row["ORDERKEY"]).zfill(10),  # ✅ 4 нуля
            "store": str(row["CONSIGNEEKEY"]),
            "qty": int(row["TOTALQTY"]) if not pd.isna(row["TOTALQTY"]) else 0,
            "susr3": str(row["SUSR3"]) if not pd.isna(row["SUSR3"]) else "",
            "ref": str(row["REFERENCENUM"]) if not pd.isna(row["REFERENCENUM"]) else ""
        })

    return orders

# ===== DATA =====
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)

# ===== PENDING =====
def load_pending():
    if os.path.exists(PENDING_FILE):
        with open(PENDING_FILE) as f:
            return json.load(f)
    return {}

def save_pending(data):
    with open(PENDING_FILE, "w") as f:
        json.dump(data, f)

# ===== LOG =====
def log_to_excel(user, orders):
    rows = []
    for o in orders:
        rows.append({
            "user": user,
            "order": o["order"],
            "store": o["store"],
            "reference": o["ref"],  # ✅ добавлено
            "time": time.strftime("%Y-%m-%d %H:%M:%S")
        })

    df_new = pd.DataFrame(rows)

    if os.path.exists(LOG_FILE):
        df_old = pd.read_excel(LOG_FILE)
        df = pd.concat([df_old, df_new])
    else:
        df = df_new

    df.to_excel(LOG_FILE, index=False)

# ===== PRIORITY =====
def get_priority(store):
    if store.startswith("25"):
        return 1
    elif store.startswith("412") or store.startswith("413"):
        return 2
    elif store.startswith("42") or store.startswith("41"):
        return 3
    elif store.startswith("496"):
        return 4
    return 4

# ===== CLEAN TTL =====
def clean_expired(data):
    now = time.time()
    for user in list(data.keys()):
        data[user] = [o for o in data[user] if now - o["time"] < TTL]
        if not data[user]:
            del data[user]
    return data

# ===== COUNT WORKERS =====
def count_workers(data):
    store_workers = {}
    for user in data:
        stores = set([o["store"] for o in data[user]])
        for s in stores:
            store_workers[s] = store_workers.get(s, 0) + 1
    return store_workers

# ===== ASSIGN =====
def assign_orders(user):
    pending = load_pending()

    # ✅ вернуть старые если есть
    if user in pending:
        return pending[user], False, False

    data = load_data()
    data = clean_expired(data)

    ORDERS = load_orders()

    used = set()
    for u in data:
        used.update([o["order"] for o in data[u]])

    available = [o for o in ORDERS if o["order"] not in used]

    stores = {}
    store_qty = {}

    for o in available:
        s = o["store"]
        stores.setdefault(s, []).append(o)
        store_qty[s] = store_qty.get(s, 0) + o["qty"]

    if not stores:
        return [], False, False

    store_workers = count_workers(data)

    store_list = sorted(stores.keys(), key=lambda s: get_priority(s))

    valid = []

    for s in store_list:
        workers = store_workers.get(s, 0)
        qty = store_qty[s]

        if qty >= BIG_STORE_THRESHOLD:
            if workers < MAX_WORKERS_PER_STORE:
                valid.append(s)
        else:
            if workers == 0:
                valid.append(s)

    if not valid:
        return [], False, False

    best_p = get_priority(valid[0])
    best = [s for s in valid if get_priority(s) == best_p]

    chosen = random.choice(best)

    orders = stores[chosen]
    total_qty = store_qty[chosen]

    BIG_STORE = total_qty >= BIG_STORE_THRESHOLD

    extra_orders = []
    SECOND_STORE = False

    if total_qty <= SMALL_STORE_THRESHOLD:
        for s in stores:
            if s == chosen:
                continue
            if store_qty[s] <= SMALL_STORE_THRESHOLD and store_workers.get(s, 0) == 0:
                extra_orders = stores[s]
                SECOND_STORE = True
                break

    assigned = orders + extra_orders

    # ✅ сохраняем как pending
    pending[user] = assigned
    save_pending(pending)

    return assigned, BIG_STORE, SECOND_STORE

# ===== CONFIRM =====
def confirm_orders(user):
    pending = load_pending()
    data = load_data()

    if user not in pending:
        return

    orders = pending[user]
    now = time.time()

    data[user] = []
    for o in orders:
        data[user].append({
            "order": o["order"],
            "store": o["store"],
            "time": now
        })

    save_data(data)
    log_to_excel(user, orders)

    del pending[user]
    save_pending(pending)

# ===== STATS =====
def get_stats():
    data = load_data()
    return {u: len(data[u]) for u in data}

# ===== HTML =====
HTML = """
<h2>📦 Dystrybucja zamówień (распределение заказов)</h2>

<form method="post">
    <input name="user" placeholder="Wpisz ID (введи ID)" required>
    <button name="action" value="get">
        Pobierz zamówienia (получить заказы)
    </button>
</form>

{% if orders %}
<h3>👤 {{user}}</h3>

<form method="post">
    <input type="hidden" name="user" value="{{user}}">
    <button name="action" value="confirm">
        ✅ Potwierdź odbiór (подтвердить получение)
    </button>
</form>

{% set grouped = {} %}
{% for o in orders %}
    {% if o.store not in grouped %}
        {% set _ = grouped.update({o.store: []}) %}
    {% endif %}
    {% set _ = grouped[o.store].append(o) %}
{% endfor %}

{% for store, items in grouped.items() %}
<h3>🏬 Sklep (магазин): {{store}}</h3>
<ul>
{% for i in items %}
<li>{{i.order}} ({{i.susr3}})</li>
{% endfor %}
</ul>
{% endfor %}
{% endif %}

<hr>
<h3>📊 Statystyka (статистика)</h3>
{% for u, c in stats.items() %}
<div>{{u}} → {{c}} zamówień (заказов)</div>
{% endfor %}
"""

# ===== ROUTE =====
@app.route("/", methods=["GET", "POST"])
def index():
    user = request.form.get("user")
    action = request.form.get("action")

    orders, big, second = [], False, False

    if user:
        if action == "confirm":
            confirm_orders(user)
        else:
            orders, big, second = assign_orders(user)

    stats = get_stats()

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
        stats=stats,
        big=big,
        second=second
    )

# ===== RUN =====
app.run(host="0.0.0.0", port=5000)
