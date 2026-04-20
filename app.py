from flask import Flask, request, render_template_string
import json, os, time, threading
import redis
import psycopg2
import pandas as pd
import gspread
from psycopg2.extras import execute_values
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)
print("🚀 NEW VERSION LOADED:", time.time())

# ===== CONFIG =====
PENDING_TTL = 1200
BIG_STORE_THRESHOLD = 1500
SMALL_STORE_THRESHOLD = 600

r = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)
DB_URL = os.environ.get("DATABASE_URL")

# ===== PRIORITY =====
PRIORITY_ORDER = [
    "25","451","495","411","498","44",
    "412","413","415","416","497","43",
    "424","421","496"
]

def get_store_priority(store):
    for i, prefix in enumerate(PRIORITY_ORDER):
        if store.startswith(prefix):
            return i
    return 999

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

# ===== LOG WORKER =====
def log_worker():
    sheet = connect_sheet().worksheet("log")

    while True:
        item = r.rpoplpush("log_queue", "log_processing")

        if item:
            data = json.loads(item)

            if r.sismember("logged_ids", data.get("id")):
                r.lrem("log_processing", 1, item)
                continue

            try:
                rows = []
                for o in data["orders"]:
                    rows.append([
                        data["user"],
                        o["order"],
                        o["store"],
                        o["ref"],
                        time.strftime("%Y-%m-%d %H:%M:%S")
                    ])

                sheet.append_rows(rows)

                r.sadd("logged_ids", data.get("id"))
                r.lrem("log_processing", 1, item)

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

    cur.execute("TRUNCATE TABLE orders")

    data = []
    for _, row in df.iterrows():
        try:
            data.append((
                str(row["ORDERKEY"]).zfill(10),
                str(row["CONSIGNEEKEY"]),
                int(row["TOTALQTY"]),
                str(row["SUSR3"] or ""),
                str(row["REFERENCENUM"] or "")
            ))
        except:
            pass

    execute_values(cur,
        "INSERT INTO orders (order_id, store, qty, susr3, ref) VALUES %s",
        data
    )

    conn.commit()
    conn.close()

# ===== LOAD (с возвратом через 20 минут) =====
def load_orders():
    conn = get_conn()
    cur = conn.cursor()

    # 🔥 ОЧИСТКА ЗАВИСШИХ STORE
    cur.execute("""
        DELETE FROM store_locks
        WHERE locked_at < NOW() - INTERVAL '20 minutes'
    """)

    conn.commit()

    # ===== ОСНОВНОЙ SELECT =====
    cur.execute("""
        SELECT order_id, store, qty, susr3, ref
        FROM orders
        WHERE assigned = FALSE
        AND (
            assigned_at IS NULL
            OR assigned_at < NOW() - INTERVAL '20 minutes'
        )
    """)

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

# ===== ASSIGN =====
def assign_orders(user):
    pending = r.get(f"pending:{user}")
    if pending:
        return json.loads(pending), False, False

    # ===== 🔥 FALLBACK ИЗ БД =====
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, store, qty, susr3, ref
        FROM orders
        WHERE assigned = FALSE
          AND assigned_to = %s
          AND assigned_at > NOW() - INTERVAL '20 minutes'
    """, (user,))

    rows = cur.fetchall()
    conn.close()

    if rows:
        return [
            {
                "order": r[0],
                "store": r[1],
                "qty": r[2],
                "susr3": r[3] or "",
                "ref": r[4] or "",
            }
            for r in rows
        ], False, False

    # ===== ТОЛЬКО ЕСЛИ НЕТ СВОИХ ЗАКАЗОВ =====
    orders = load_orders()

    if not orders:
        return [], False, False

    stores, store_qty = {}, {}

    for o in orders:
        s = o["store"]
        stores.setdefault(s, []).append(o)
        store_qty[s] = store_qty.get(s, 0) + o["qty"]

    sorted_stores = sorted(
        stores.keys(),
        key=lambda x: (get_store_priority(x), int(x))
    )

    # ===== ВЫБОР STORE С БЛОКИРОВКОЙ =====
    chosen = None

    for s in sorted_stores:
        conn = get_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO store_locks (store, user_id, locked_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (store) DO NOTHING
                RETURNING store
            """, (s, user))

            locked = cur.fetchone()

            conn.commit()
            conn.close()

            if locked:
                chosen = s
                break

        except:
            conn.rollback()
            conn.close()
            continue

    if not chosen:
        return [], False, False

    store_orders = stores[chosen]
    total_qty = store_qty[chosen]

    # ===== ЛОГИКА РАСПРЕДЕЛЕНИЯ =====
    if total_qty >= BIG_STORE_THRESHOLD:
        replen, other = split_replen_and_other(store_orders)
        assigned = replen if replen else store_orders
    else:
        assigned = list(store_orders)

        if total_qty <= SMALL_STORE_THRESHOLD:
            for s in sorted_stores:
                if s != chosen and store_qty[s] <= SMALL_STORE_THRESHOLD:
                    assigned += stores[s]
                    break

    if not assigned:
        return [], False, False

    # ===== ВРЕМЕННЫЙ РЕЗЕРВ =====
    conn = get_conn()
    cur = conn.cursor()

    ids = [o["order"] for o in assigned]

    cur.execute("""
        UPDATE orders
        SET assigned_to = %s,
            assigned_at = NOW()
        WHERE order_id = ANY(%s)
          AND (
              assigned_to IS NULL
              OR assigned_at < NOW() - INTERVAL '20 minutes'
          )
        RETURNING order_id
    """, (user, ids))

    updated = cur.fetchall()

    conn.commit()
    conn.close()

    if len(updated) != len(ids):
        # 🔥 освобождаем store
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            DELETE FROM store_locks
            WHERE store = %s
        """, (chosen,))

        conn.commit()
        conn.close()

        return [], False, False

    # ✅ сохраняем pending
    r.setex(f"pending:{user}", PENDING_TTL, json.dumps(assigned))

    return assigned, False, False

# ===== CONFIRM =====
def confirm_orders(user):
    raw = r.get(f"pending:{user}")
    if not raw:
        return

    orders = json.loads(raw)

    conn = get_conn()
    cur = conn.cursor()

    ids = [o["order"] for o in orders]

    # подтверждаем заказы
    cur.execute("""
        UPDATE orders
        SET assigned = TRUE
        WHERE order_id = ANY(%s)
          AND assigned_to = %s
    """, (ids, user))

    # 🔥 ОСВОБОЖДАЕМ STORE (ВАЖНО)
    stores = list(set(o["store"] for o in orders))

    for s in stores:
        cur.execute("""
            DELETE FROM store_locks
            WHERE store = %s
        """, (s,))

    conn.commit()
    conn.close()

    r.delete(f"pending:{user}")

    r.rpush("log_queue", json.dumps({
        "id": f"{user}_{time.time()}",
        "user": user,
        "orders": orders
    }))

HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body {
    font-family: Arial;
    padding: 10px;
    background: #f5f5f5;
}

h2 {
    font-size: 20px;
}

input {
    width: 100%;
    padding: 15px;
    font-size: 18px;
    margin-bottom: 10px;
}

button {
    width: 100%;
    padding: 15px;
    font-size: 18px;
    margin-bottom: 10px;
}

.order {
    background: white;
    padding: 10px;
    margin-bottom: 8px;
    border-radius: 8px;
}

/* 🔴 мигающее предупреждение */
@keyframes blink {
    0% { background: red; }
    50% { background: darkred; }
    100% { background: red; }
}

.alert {
    color: white;
    padding: 12px;
    font-weight: bold;
    text-align: center;
    margin-bottom: 12px;
    border-radius: 10px;
    animation: blink 1s infinite;
    font-size: 16px;
}

/* 📳 тряска */
@keyframes shake {
  0% { transform: translateX(0); }
  20% { transform: translateX(-6px); }
  40% { transform: translateX(6px); }
  60% { transform: translateX(-6px); }
  80% { transform: translateX(6px); }
  100% { transform: translateX(0); }
}

/* усиленный alert */
.alert.active {
    animation: blink 1s infinite, shake 0.4s infinite;
    background: #ff0000;
}

/* 🟢 успешное подтверждение */
.success {
    background: #28a745;
    color: white;
    padding: 12px;
    font-weight: bold;
    text-align: center;
    margin-bottom: 12px;
    border-radius: 10px;
    font-size: 16px;
}
</style>
</head>

<body>

<h2>📦 Dystrybucja zamówień</h2>

<form method="post">
    <input name="user" placeholder="Wpisz ID" required autofocus>
    <button name="action" value="get">Pobierz zamówienia</button>
</form>

{% if user == "admin" %}
<form method="post" enctype="multipart/form-data">
    <input type="hidden" name="user" value="{{user}}">
    <input type="file" name="file" required>
    <button name="action" value="upload">📤 Upload Excel</button>
</form>
{% endif %}

{% if success %}
<div class="success">
✅ Zamówienie zostało potwierdzone
</div>
{% endif %}

{% if orders %}

<div class="alert">
⚠️ PAMIĘTAJ: MUSISZ POTWIERDZIĆ ZAMÓWIENIE!
</div>

<h3>👤 {{user}}</h3>

<form method="post">
    <input type="hidden" name="user" value="{{user}}">
    <button name="action" value="confirm">✅ Potwierdź odbiór</button>
</form>

{% for o in orders %}
<div class="order">
{{o.order}} | {{o.store}} | {{o.qty}} | {{o.susr3}}
</div>
{% endfor %}

{% endif %}

{% if no_orders %}
<div style="color:gray; margin-top:20px;">
    ❌ Brak dostępnych zamówień do pobrania
</div>
{% endif %}

<script>
let hasOrders = {{ 'true' if orders else 'false' }};
let confirmed = {{ 'true' if success else 'false' }};

let WARNING_TIME = 2 * 60 * 1000;
let triggered = false;
let vibrationInterval = null;

// 🚫 если нет заказов или уже подтвердил → ничего не делаем
if (!hasOrders || confirmed) {
    console.log("NO WARNING");
} else {

    let startTime = Date.now();

    let timer = setInterval(() => {
        if (triggered) return;

        let now = Date.now();

        if (now - startTime > WARNING_TIME) {
            triggered = true;
            triggerWarning();
        }
    }, 1000);

    function triggerWarning() {
        let alertBox = document.querySelector('.alert');

        if (alertBox) {
            alertBox.classList.add('active');
        }

        startVibration();
    }

    function startVibration() {
        if ("vibrate" in navigator) {
            vibrationInterval = setInterval(() => {
                navigator.vibrate([300, 200, 300, 200, 500]);
            }, 5000);
        }
    }
}
</script>

// 🔴 усиление
function triggerWarning() {
    let alertBox = document.querySelector('.alert');

    if (alertBox) {
        alertBox.classList.add('active');
        alertBox.innerText = "⚠️ PAMIĘTAJ: MUSISZ POTWIERDZIĆ ZAMÓWIENIE!";
    }

    startVibration();
}

// 📳 вибрация
function startVibration() {
    if ("vibrate" in navigator) {
        setInterval(() => {
            navigator.vibrate([300, 200, 300, 200, 500]);
        }, 5000);
    }
}
</script>

</body>
</html>
"""

# ===== ROUTE =====
@app.route("/", methods=["GET", "POST"])
def index():
    user = request.form.get("user")
    action = request.form.get("action")
    file = request.files.get("file")

    orders = []
    no_orders = False
    success = False   # 👈 ВОТ ЭТА СТРОКА

if user:
    if action == "upload" and user == "admin" and file:
        upload_orders(file)
    elif action == "confirm":
        confirm_orders(user)
        success = True
        orders = []
    else:
        orders, _, _ = assign_orders(user)

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
        no_orders=no_orders
    )

app.run(host="0.0.0.0", port=5000)
