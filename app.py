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
r = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)
DB_URL = os.environ.get("DATABASE_URL")
SPLIT_KEY = "split_queue"
SPLIT_TTL = 1200  # 20 минут

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

    print("GSHEET STEP 1")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    print("GSHEET STEP 2")

    creds = json.loads(
        os.environ.get("GOOGLE_CREDENTIALS")
    )

    print("GSHEET STEP 3")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds,
        scope
    )

    print("GSHEET STEP 4")

    client = gspread.authorize(creds)

    print("GSHEET STEP 5")

    sheet = client.open_by_key(
        "1dtSO224vSpxaR5Jm3wNQ09SiMjSjLGkgL1C4lRfg7YM"
    )

    print("GSHEET STEP 6")

    return sheet


def get_valid_users():

    try:

        print("HC LOAD START")

        sheet = connect_sheet().worksheet("HC")

        print("HC SHEET OPENED")

        values = sheet.col_values(1)

        print("HC ROWS:", len(values))

        users = set()

        for v in values[1:]:

            v = str(v).strip()

            if v:
                users.add(v)

        print("HC USERS LOADED:", len(users))

        return users

    except Exception as e:

        print("HC LOAD ERROR:", repr(e))

        return set()


def refresh_valid_users():

    try:

        print("HC CACHE REFRESH START")

        sheet = connect_sheet().worksheet("HC")

        print("HC SHEET OPENED")

        values = sheet.col_values(1)

        print("HC ROWS:", len(values))

        r.delete("valid_users")

        count = 0

        for v in values[1:]:

            v = str(v).strip()

            if v:

                r.sadd("valid_users", v)

                count += 1

        print("VALID USERS UPDATED:", count)

    except Exception as e:

        print("VALID USERS ERROR:", repr(e))
        
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
                        o.get("order", ""),
                        o.get("store", ""),
                        o.get("ref", ""),
                        time.strftime("%Y-%m-%d %H:%M:%S"),
                        data.get("status", "CONFIRMED")
                    ])

                sheet.append_rows(rows)

                r.sadd("logged_ids", data.get("id"))

                r.lrem("log_processing", 1, item)

            except Exception as e:

                print("LOG ERROR:", e)

                time.sleep(2)

        time.sleep(1)


threading.Thread(
    target=log_worker,
    daemon=True
).start()


# ===== VALID USERS CACHE =====
def valid_users_worker():

    print("VALID USERS WORKER STARTED")

    while True:

        time.sleep(300)


# threading.Thread(
#     target=valid_users_worker,
#     daemon=True
# ).start()

# ===== TYPE DETECTION =====
def detect_order_type(susr3):

    susr3 = (susr3 or "").upper()

    if "TOP STORE" in susr3:
        return "TOP_STORE"

    elif "REPLEN" in susr3:
        return "REPLENISHMENT"

    else:
        return "NEW_LINES"

# ===== UPLOAD =====
def upload_orders(file, forced_type=None):

    df = pd.read_excel(
        file,
        engine="openpyxl",
        dtype=str
    )

    conn = get_conn()
    cur = conn.cursor()

    data = []

    for _, row in df.iterrows():

        try:

            susr3 = str(row["SUSR3"] or "")

            order_type = (
                forced_type
                if forced_type
                else detect_order_type(susr3)
            )

            base_id = str(row["ORDERKEY"]).zfill(10)

            order_id = f"{base_id}_{order_type}"

            qty = int(float(row["TOTALQTY"] or 0))
            lines = int(float(row["TOTALORDERLINES"] or 0))

            data.append((
                order_id,
                str(row["CONSIGNEEKEY"]),
                qty,
                lines,
                susr3,
                str(row["REFERENCENUM"] or ""),
                order_type
            ))

        except Exception as e:

            print("UPLOAD ERROR:", e)

    execute_values(
        cur,
        """
        INSERT INTO orders
        (
            order_id,
            store,
            qty,
            total_lines,
            susr3,
            ref,
            order_type
        )
        VALUES %s
        """,
        data
    )

    conn.commit()
    conn.close()

# ===== LOAD =====
def load_orders():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM store_locks
        WHERE locked_at < NOW() - INTERVAL '40 minutes'
    """)
    conn.commit()
    cur.execute("""
        SELECT order_id, store, qty, total_lines, susr3, ref, order_type
        FROM orders
        WHERE assigned = FALSE
          AND (
              assigned_to IS NULL
              OR assigned_at < NOW() - INTERVAL '40 minutes'
          )
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "order": r[0],
            "store": r[1],
            "qty": r[2],
            "lines": r[3],
            "susr3": r[4] or "",
            "ref": r[5] or "",
            "order_type": r[6],
        }
        for r in rows
    ]

# ===== SPLIT =====
MAX_STORES = 2

def split_replen_and_other(orders):

    replen = []
    other = []

    for o in orders:

        if "replen" in (o["susr3"] or "").lower():
            replen.append(o)

        else:
            other.append(o)

    return replen, other


def try_lock(store, user):

    conn = get_conn()
    cur = conn.cursor()

    try:

        cur.execute("""
            INSERT INTO store_locks (store, user_id, locked_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (store) DO NOTHING
            RETURNING store
        """, (store, user))

        ok = cur.fetchone()

        conn.commit()
        conn.close()

        return ok is not None

    except:

        conn.rollback()
        conn.close()

        return False


# ===== MAIN ASSIGN =====
def assign_orders(user, order_type):

    pending = r.get(f"pending:{user}")

    if pending:

        pending_orders = json.loads(pending)

        if pending_orders:

            pending_type = (
                pending_orders[0]
                .get("order_type", "")
                .upper()
            )

            requested_type = (
                order_type or ""
            ).strip().upper()

            # тот же тип → вернуть старые заказы
            if pending_type == requested_type:

                return pending_orders, False, False

            # =====================================
            # RELEASE OLD PENDING
            # =====================================
            conn = get_conn()
            cur = conn.cursor()

            ids = [
                o["order"]
                for o in pending_orders
            ]

            cur.execute("""
                UPDATE orders
                SET assigned_to = NULL,
                    assigned_at = NULL
                WHERE order_id = ANY(%s)
                  AND assigned = FALSE
                  AND assigned_to = %s
            """, (ids, user))

            stores = list(set(
                o["store"]
                for o in pending_orders
            ))

            for s in stores:

                cur.execute("""
                    DELETE FROM store_locks
                    WHERE store = %s
                """, (s,))

            conn.commit()
            conn.close()

            r.delete(f"pending:{user}")


    # =========================================
    # SPLIT QUEUE
    # =========================================
    split_raw = r.get(SPLIT_KEY)

    if split_raw:

        batch = json.loads(split_raw)

        if batch:

            ids = [o["order"] for o in batch]

            conn = get_conn()
            cur = conn.cursor()

            cur.execute("""
                UPDATE orders
                SET assigned_to = %s,
                    assigned_at = NOW()
                WHERE order_id = ANY(%s)
                AND assigned_to IS NULL
                RETURNING order_id
            """, (user, ids))

            updated = cur.fetchall()

            conn.commit()
            conn.close()

            if len(updated) != len(ids):

                r.delete(SPLIT_KEY)

                return [], False, False

            r.delete(SPLIT_KEY)

            r.setex(
                f"pending:{user}",
                PENDING_TTL,
                json.dumps(batch)
            )

            return batch, False, False


    # =========================================
    # LOAD ORDERS
    # =========================================
    orders = load_orders()

    if not orders:
        return [], False, False

    order_type = (order_type or "").strip().upper()


    # =========================================
    # FILTER TYPE
    # =========================================
    orders = [
        o for o in orders
        if (o.get("order_type") or "").upper() == order_type
    ]

    if not orders:
        return [], False, False


    # =========================================
    # GROUP BY STORE
    # =========================================
    stores = {}
    store_lines = {}

    for o in orders:

        s = o["store"]

        stores.setdefault(s, []).append(o)

        store_lines[s] = (
            store_lines.get(s, 0)
            + (o.get("lines") or 0)
        )


    # =========================================
    # SORT BY PRIORITY
    # =========================================
    sorted_stores = sorted(
        stores.keys(),
        key=lambda s: (
            get_store_priority(s),
            s
        )
    )


    # =========================================
    # TAKE FIRST AVAILABLE STORE
    # =========================================
    assigned = []
    locked_store = None

    for store in sorted_stores:

        if not try_lock(store, user):
            continue

        locked_store = store

        store_orders = stores[store]
        total_lines = store_lines[store]


        # =====================================
        # SPLIT ONLY BIG REPLEN
        # =====================================
        if (
            order_type == "REPLENISHMENT"
            and total_lines >= 1500
        ):

            replen, other = split_replen_and_other(store_orders)

            if replen and other:

                assigned = replen

                r.setex(
                    SPLIT_KEY,
                    SPLIT_TTL,
                    json.dumps(other)
                )

            else:
                assigned = store_orders

        else:

            assigned = store_orders

        break


    # =========================================
    # NOTHING FOUND
    # =========================================
    if not assigned:
        return [], False, False


    # =========================================
    # SAVE ASSIGNMENT
    # =========================================
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
            OR assigned_at < NOW() - INTERVAL '40 minutes'
        )
        RETURNING order_id
    """, (user, ids))

    updated = cur.fetchall()

    conn.commit()
    conn.close()


    # =========================================
    # ROLLBACK IF CONFLICT
    # =========================================
    if len(updated) != len(ids):

        conn = get_conn()
        cur = conn.cursor()

        if locked_store:
            cur.execute("""
                DELETE FROM store_locks
                WHERE store = %s
            """, (locked_store,))

        conn.commit()
        conn.close()

        return [], False, False


    # =========================================
    # SAVE PENDING
    # =========================================
    r.setex(
        f"pending:{user}",
        PENDING_TTL,
        json.dumps(assigned)
    )

    return assigned, False, False

# ===== MY ORDERS =====
def get_user_orders(user):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            order_id,
            store,
            qty,
            total_lines,
            susr3
        FROM orders
        WHERE assigned_to = %s
        AND assigned_at > NOW() - INTERVAL '1 hour'
        ORDER BY assigned_at DESC
    """, (user,))

    rows = cur.fetchall()

    conn.close()

    return [
        {
            "order": r[0],
            "store": r[1],
            "qty": r[2],
            "lines": r[3],
            "susr3": r[4] or ""
        }
        for r in rows
    ]

# ===== CONFIRM =====
def confirm_orders(user):
    raw = r.get(f"pending:{user}")
    if not raw:
        return

    orders = json.loads(raw)
    conn = get_conn()
    cur = conn.cursor()

    ids = [o["order"] for o in orders]

    cur.execute("""
        UPDATE orders
        SET assigned = TRUE
        WHERE order_id = ANY(%s)
          AND assigned_to = %s
    """, (ids, user))

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


# ===== DASHBOARD DATA =====
@app.route("/dashboard_data")
def dashboard_data():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT assigned_to, store, total_lines, qty, assigned_at
        FROM orders
        WHERE assigned = FALSE
          AND assigned_to IS NOT NULL
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT
            COUNT(*),
            COALESCE(SUM(total_lines), 0),
            COALESCE(SUM(qty), 0)
        FROM orders
        WHERE assigned = FALSE
    """)
    total_orders, total_lines, total_qty = cur.fetchone()

    conn.close()

    workers = {}

    for worker, store, lines, qty, assigned_at in rows:

        if not worker:
            continue

        workers.setdefault(worker, {
            "lines": 0,
            "qty": 0,
            "orders": 0,
            "stores": set(),
            "pending": False,
            "oldest": assigned_at
        })

        workers[worker]["lines"] += int(lines or 0)
        workers[worker]["qty"] += int(qty or 0)
        workers[worker]["orders"] += 1
        workers[worker]["stores"].add(store)

        if assigned_at and workers[worker]["oldest"]:
            if assigned_at < workers[worker]["oldest"]:
                workers[worker]["oldest"] = assigned_at

        workers[worker]["pending"] = True

    for w in workers:
        workers[w]["stores"] = list(workers[w]["stores"])

    return {
        "workers": workers,
        "total_orders": int(total_orders or 0),
        "total_lines": int(total_lines or 0),
        "total_qty": int(total_qty or 0)
    }

# ===== RESET SYSTEM =====
@app.route("/reset", methods=["POST"])
def reset_system():
    user = request.form.get("user") or request.headers.get("X-USER")
    if user != "admin":
        return {"status": "error", "message": "Unauthorized"}, 403

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE orders RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE store_locks")
        conn.commit()
        conn.close()

        for key in r.keys("pending:*"):
            r.delete(key)
        r.delete(SPLIT_KEY)

        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== RELOAD HC =====
@app.route("/reload_hc", methods=["POST"])
def reload_hc():

    user = request.form.get("user") or request.headers.get("X-USER")

    if user != "admin":
        return {
            "status": "error",
            "message": "Unauthorized"
        }, 403

    try:

        refresh_valid_users()

        count = r.scard("valid_users")

        return {
            "status": "ok",
            "users": count
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# ===== RELEASE USER =====
@app.route("/release_user", methods=["POST"])
def release_user():

    admin = request.form.get("admin")
    worker = request.form.get("worker")

    if admin != "admin":
        return {
            "status": "error",
            "message": "Unauthorized"
        }, 403

    try:

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT store
            FROM orders
            WHERE assigned_to = %s
              AND assigned = FALSE
        """, (worker,))

        stores = [r[0] for r in cur.fetchall()]

        cur.execute("""
            UPDATE orders
            SET assigned_to = NULL,
                assigned_at = NULL
            WHERE assigned_to = %s
              AND assigned = FALSE
        """, (worker,))

        for s in stores:

            cur.execute("""
                DELETE FROM store_locks
                WHERE store = %s
            """, (s,))

        conn.commit()
        conn.close()

        r.delete(f"pending:{worker}")

        return {"status": "ok"}

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# ===== DASHBOARD UI =====
@app.route("/dashboard")
def dashboard():
    return """
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body { font-family: Arial; background:#f5f5f5; padding:10px; }
.card { background:white; padding:12px; margin-bottom:10px; border-radius:10px; }
.big { font-size:20px; font-weight:bold; }
.red { background:#ffcccc; }
.yellow { background:#fff3cd; }
.green { background:#d4edda; }
.pending { border: 3px solid red; }
button { margin-bottom:10px; padding:10px; font-size:16px; background:#ff4444; color:white; border:none; border-radius:6px; }
</style>
</head>
<body>
<h2>📊 Dashboard</h2>
<button onclick="resetSystem()">🔄 RESET SYSTEM</button>
<button onclick="reloadHC()">👥 RELOAD HC</button>
<button onclick="togglePriority()">⭐ LOAD PRIORITY</button>
<div id="summary" class="card"></div>

<div id="priority-box"
     style="display:none;
            margin:15px 0;
            background:white;
            padding:15px;
            border-radius:10px;">

    <h3>⭐ Priority List</h3>

    <textarea
        id="priority-text"
        style="
            width:100%;
            height:300px;
            font-size:15px;
            font-family:monospace;
        "
        placeholder="Вставьте список магазинов..."
    ></textarea>

    <button onclick="savePriority()">
        💾 SAVE PRIORITY
    </button>

</div>

<div id="workers"></div>
<script>
function getColor(lines){
    if (lines > 450) return "red";
    if (lines >= 300) return "yellow";
    return "green";
}
function formatTime(ts){
    if (!ts) return "";
    let d = new Date(ts);
    return d.toLocaleTimeString();
}
async function resetSystem() {
    let user = prompt("Введите admin ID:");
    if (!user) { alert("❌ Отменено"); return; }
    if (!confirm("⚠️ Ты уверен что хочешь удалить ВСЕ заказы?")) return;
    let res = await fetch('/reset', {
        method: 'POST',
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ user: user })
    });
    let data = await res.json();
    if (data.status === "ok") {
        alert("✅ Система очищена");
    } else {
        alert("❌ Ошибка: " + data.message);
    }
    load();
}
async function reloadHC() {

    let user = prompt("Введите admin ID:");

    if (!user) {
        alert("❌ Отменено");
        return;
    }

    let res = await fetch('/reload_hc', {
        method: 'POST',
        headers: {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        body: new URLSearchParams({
            user: user
        })
    });

    let data = await res.json();

    if (data.status === "ok") {

        alert(
            "✅ HC загружен. Пользователей: "
            + data.users
        );

    } else {

        alert(
            "❌ Ошибка: "
            + data.message
        );
    }
}

async function releaseUser(worker) {

    let admin = prompt("Введите admin ID:");

    if (!admin) return;

    if (!confirm(
        "Освободить все заказы работника " + worker + " ?"
    )) return;

    let res = await fetch('/release_user', {
        method: 'POST',
        headers: {
            "Content-Type":
            "application/x-www-form-urlencoded"
        },
        body: new URLSearchParams({
            admin: admin,
            worker: worker
        })
    });

    let data = await res.json();

    if (data.status === "ok") {

        alert("✅ Заказы освобождены");

        load();

    } else {

        alert(
            "❌ Ошибка: " + data.message
        );
    }
}

async function load() {
    let res = await fetch('/dashboard_data');
    let data = await res.json();
    document.getElementById('summary').innerHTML =
        "<div class='big'>Remaining lines: " + data.total_lines + "</div>" +
        "<div>Remaining qty: " + data.total_qty + "</div>" +
        "<div>Remaining orders: " + data.total_orders + "</div>";
    let html = "";
    for (let w in data.workers) {
        let u = data.workers[w];
        let color = getColor(u.lines);
        let pendingClass = u.pending ? "pending" : "";
        html += "<div class='card " + color + " " + pendingClass + "'>" +
            "<b>👤 " + w + "</b><br>" +
            "Lines: " + u.lines + "<br>" +
            "Orders: " + u.orders + "<br>" +
            "Stores: " + u.stores.join(", ") + "<br>" +
            (u.pending ? "⚠️ NOT CONFIRMED" : "✅ OK") + "<br>" +
            "⏱ Since: " + formatTime(u.oldest) + "<br>" +
            "<button data-user='" + w + "' class='release-btn'>" +
            "🔓 RELEASE" +
            "</button>" +
            "</div>";
    }
    document.getElementById('workers').innerHTML = html;

    document.querySelectorAll('.release-btn').forEach(btn => {

    btn.onclick = function() {

        releaseUser(
            this.getAttribute('data-user')
        );

    };

});
}
load();
setInterval(load, 5000);
</script>
</body>
</html>
"""


HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body { font-family: Arial; padding: 10px; background: #f5f5f5; }
h2 { font-size: 20px; }
input { width: 100%; padding: 15px; font-size: 18px; margin-bottom: 10px; }
button { width: 100%; padding: 15px; font-size: 18px; margin-bottom: 10px; }
.order { background: white; padding: 10px; margin-bottom: 8px; border-radius: 8px; }
@keyframes blink {
    0% { background: red; }
    50% { background: darkred; }
    100% { background: red; }
}
.alert {
    color: white; padding: 12px; font-weight: bold; text-align: center;
    margin-bottom: 12px; border-radius: 10px;
    animation: blink 1s infinite; font-size: 16px;
}
@keyframes shake {
    0% { transform: translateX(0); }
    20% { transform: translateX(-6px); }
    40% { transform: translateX(6px); }
    60% { transform: translateX(-6px); }
    80% { transform: translateX(6px); }
    100% { transform: translateX(0); }
}
.alert.active {
    animation: blink 1s infinite, shake 0.4s infinite;
    background: #ff0000;
}
.success {
    background: #28a745; color: white; padding: 12px;
    font-weight: bold; text-align: center; margin-bottom: 12px;
    border-radius: 10px; font-size: 16px;
}
</style>
</head>
<body>
<h2>📦 Dystrybucja zamówień</h2>
<form method="post" onkeydown="return event.key != 'Enter';">
    <input name="user" placeholder="Wpisz ID" required autofocus>
    <select name="type" style="width:100%; padding:15px; font-size:16px; margin-bottom:10px;">
        <option value="REPLENISHMENT">Replenishment</option>
        <option value="NEW_LINES">NEW LINES</option>
        <option value="TOP_STORE">TOP STORE</option>
    </select>
    <button name="action" value="get">Pobierz zamówienia</button>
</form>

<button type="button" onclick="toggleHandover()">
    🔄 Dokończenie zamówienia
</button>

<button type="button" onclick="toggleMyOrders()">
    📋 Moje zamówienia
</button>

<div id="myorders-box" style="display:none; margin-top:10px;">

<form method="post">

    <input
        name="user"
        placeholder="Wpisz ID"
        required
        style="margin-bottom:10px;"
    >

    <input
        type="hidden"
        name="action"
        value="my_orders"
    >

    <button type="submit">
        📋 Pokaż moje zamówienia
    </button>

</form>

</div>

<div id="handover-box" style="display:none; margin-top:10px;">

    <form method="post" onkeydown="return event.key != 'Enter';">

        <input
            name="user"
            placeholder="Wpisz ID"
            required
            style="margin-bottom:10px;"
        >

        <input type="hidden" name="action" value="handover">

        {% for i in range(14) %}
        <input
            name="handover{{i}}"
            placeholder="Scan order"
            style="margin-bottom:5px;"
        >
        {% endfor %}

        <button type="submit">
            📤 Wyślij
        </button>

    </form>

</div>

{% if user == "admin" %}
<form method="post" enctype="multipart/form-data">
    <input type="hidden" name="user" value="{{user}}">
    <input type="file" name="file" required>
    <button name="action" value="upload_replen">📤 Upload Replenishment</button>
    <button name="action" value="upload_new">📤 Upload NEW LINES</button>
    <button name="action" value="upload_top">📤 Upload TOP STORE</button>
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
    {{o.order.split('_')[0]}} | {{o.store}} | {{o.qty}} | {{o.susr3}}
</div>
{% endfor %}
{% endif %}

{% if invalid_user %}
<div style="
    background:#ffcccc;
    color:#990000;
    padding:12px;
    border-radius:8px;
    margin-top:20px;
    font-weight:bold;
">
    ❌ Nieprawidłowy ID użytkownika
</div>
{% endif %}

{% if no_orders %}
<div style="color:gray; margin-top:20px;">
    ❌ Brak dostępnych zamówień do pobrania
</div>
{% endif %}

<script>

function toggleHandover() {

    let box = document.getElementById("handover-box");

    if (box.style.display === "none") {
        box.style.display = "block";
    } else {
        box.style.display = "none";
    }
}

function toggleMyOrders() {

    let box = document.getElementById("myorders-box");

    if (box.style.display === "none") {

        box.style.display = "block";

    } else {

        box.style.display = "none";

    }
}

let hasPending =
    {{ 'true' if pending_exists else 'false' }};

let WARNING_TIME = 2 * 60 * 1000;
let triggered = false;

if (!hasPending) {

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
            setInterval(() => {
                navigator.vibrate([300, 200, 300, 200, 500]);
            }, 5000);
        }
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
    success = False

    if user:

        if user != "admin" and not r.sismember(
            "valid_users",
            str(user).strip()
        ):

            return render_template_string(
                HTML,
                orders=[],
                user=user,
                no_orders=False,
                success=False,
                invalid_user=True
            )


        # ===== UPLOAD =====
        if user == "admin" and file:
            if action == "upload_replen":
                upload_orders(file, forced_type="REPLENISHMENT")
            elif action == "upload_new":
                upload_orders(file, forced_type="NEW_LINES")
            elif action == "upload_top":
                upload_orders(file, forced_type="TOP_STORE")
                
        # ===== CONFIRM =====
        elif action == "confirm":
            confirm_orders(user)
            success = True
            orders = []

        # ===== HANDOVER =====
        elif action == "handover":

            scanned = []

            for i in range(14):

                order_id = request.form.get(f"handover{i}")

                if order_id:

                    scanned.append({
                        "order": order_id.strip(),
                        "store": "",
                        "ref": ""
                    })

            if scanned:

                r.rpush("log_queue", json.dumps({
                    "id": f"handover_{user}_{time.time()}",
                    "user": user,
                    "status": "DOKONCZENIE",
                    "orders": scanned
                }))        

        # ===== MY ORDERS =====
        elif action == "my_orders":

            orders = get_user_orders(user)

        # ===== GET ORDERS =====
        else:

            # ❌ admin не получает заказы
            if user == "admin":
                orders = []

            else:
                order_type = request.form.get("type") or "REPLENISHMENT"
                orders, _, _ = assign_orders(user, order_type)

                if action == "get" and user and not orders:
                    no_orders = True

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
        no_orders=no_orders,
        success=success,
        pending_exists=bool(
            user and r.get(f"pending:{user}")
        )
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
