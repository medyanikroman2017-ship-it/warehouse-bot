from flask import Flask, request, render_template_string
import json, os, time, threading
import redis
import psycopg2
import pandas as pd
import gspread
import random
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
                int(row["TOTALORDERLINES"]) if pd.notna(row["TOTALORDERLINES"]) else 0,
                str(row["SUSR3"] or ""),
                str(row["REFERENCENUM"] or "")
            ))
        except:
            pass
    execute_values(
        cur,
        "INSERT INTO orders (order_id, store, qty, total_lines, susr3, ref) VALUES %s",
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
        WHERE locked_at < NOW() - INTERVAL '20 minutes'
    """)
    conn.commit()

    cur.execute("""
        SELECT order_id, store, qty, total_lines, susr3, ref
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
            "lines": r[3],
            "susr3": r[4] or "",
            "ref": r[5] or "",
        }
        for r in rows
    ]


# ===== SPLIT =====

MAX_STORES = 2

def split_replen_and_other(orders):
    replen, other = [], []
    for o in orders:
        if "replen" in o["susr3"].lower():
            replen.append(o)
        else:
            other.append(o)
    return replen, other


def assign_orders(user):
    pending = r.get(f"pending:{user}")
    if pending:
        return json.loads(pending), False, False

    # ===== SPLIT QUEUE =====
    split_raw = r.get(SPLIT_KEY)
    if split_raw:
        batch = json.loads(split_raw)
        r.delete(SPLIT_KEY)

        r.setex(f"pending:{user}", PENDING_TTL, json.dumps(batch))
        return batch, False, False

    # ===== FALLBACK =====
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, store, qty, total_lines, susr3, ref
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
                "lines": r[3],
                "susr3": r[4] or "",
                "ref": r[5] or "",
            }
            for r in rows
        ], False, False

    # ===== LOAD =====
    orders = load_orders()
    if not orders:
        return [], False, False

    # ===== GROUP =====
    stores, store_lines = {}, {}

    for o in orders:
        s = o["store"]
        stores.setdefault(s, []).append(o)
        store_lines[s] = store_lines.get(s, 0) + (o.get("lines") or 0)

    # ===== PRIORITY MAP =====
    priority_map = {}
    for s in stores:
        p = next((x for x in PRIORITY_ORDER if s.startswith(x)), "OTHER")
        priority_map.setdefault(p, []).append(s)

    # ===== ONLY 1 PRIORITY =====
    active_priority = None
    for p in PRIORITY_ORDER:
        if p in priority_map and priority_map[p]:
            active_priority = p
            break

    if not active_priority:
        return [], False, False

    group = priority_map[active_priority]

    # ===== CLASSIFY =====
    small, standard, large = [], [], []

    for s in group:
        lines = store_lines[s]

        if lines >= 1200:
            large.append(s)
        elif lines >= 200:
            standard.append(s)
        else:
            small.append(s)

    # ===== SORT =====
    standard.sort(key=lambda s: store_lines[s])
    small.sort(key=lambda s: store_lines[s])
    large.sort(key=lambda s: store_lines[s])

    assigned = []
    used = set()
    current_load = 0

    TARGET = 400
    TOLERANCE = 40
    MIN_LOAD = 300
    MAX_SMALL_ADDS = 2

    # ===== LOCK =====
    def try_lock(store):
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO store_locks (store, user_id, locked_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (store) DO NOTHING
                RETURNING store
            """, (store, user))
            locked = cur.fetchone()
            conn.commit()
            conn.close()
            return locked is not None
        except:
            conn.rollback()
            conn.close()
            return False

    # =========================
    # 🎯 WEIGHTED PICK
    # =========================
    candidates = []

    if large:
        candidates.append(("large", 0.15))
    if standard:
        candidates.append(("standard", 0.6))
    if small:
        candidates.append(("small", 0.25))

    def pick_type():
        total = sum(w for _, w in candidates)
        r_val = random.uniform(0, total)
        upto = 0
        for t, w in candidates:
            if upto + w >= r_val:
                return t
            upto += w

    picked_type = pick_type()

    # ===== SELECT POOL =====
    if picked_type == "large":
        pool = large
    elif picked_type == "standard":
        pool = standard
    else:
        if current_load == 0 and standard:
            pool = standard
        else:
            pool = small

    # ===== PICK STORE =====
    for s in pool:
        if len(used) >= MAX_STORES:
            break

    if not try_lock(s):
        continue

    replen, other = split_replen_and_other(stores[s])

    total_lines = sum(o["lines"] or 0 for o in stores[s])

    # 🔥 делим ТОЛЬКО если реально большой магазин
    if total_lines > TARGET + 100 and replen and other:

        assigned += replen
        current_load += sum(o["lines"] or 0 for o in replen)

        # остаток в очередь
        r.setex(SPLIT_KEY, SPLIT_TTL, json.dumps(other))

    else:
        # ✅ обычный случай — берем ВСЁ вместе
        assigned += stores[s]
        current_load += total_lines

    used.add(s)
    break

    # =========================
    # ➕ SMALL ADD (добор)
    # =========================
    small_added = 0

    if current_load > 0:
        for s in small:
            if s in used:
                continue

            if len(used) >= MAX_STORES:
                break

            if current_load >= TARGET:
                break

            if current_load >= MIN_LOAD and small_added >= 1:
                break

            if small_added >= MAX_SMALL_ADDS:
                break

            if try_lock(s):
                assigned += stores[s]
                current_load += store_lines[s]
                used.add(s)
                small_added += 1

    # =========================
    # 🆘 ONLY SMALL (fallback)
    # =========================
    if current_load == 0 and not standard and not large and small:
        for s in small:
            if len(used) >= MAX_STORES:
                break

            if try_lock(s):
                assigned += stores[s]
                current_load += store_lines[s]
                used.add(s)

                if current_load >= TARGET - TOLERANCE:
                    break

    # ===== CHECK =====
    if not assigned:
        return [], False, False

    # ===== DB UPDATE =====
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
        conn = get_conn()
        cur = conn.cursor()
        for s in used:
            cur.execute("DELETE FROM store_locks WHERE store = %s", (s,))
        conn.commit()
        conn.close()
        return [], False, False

    # ===== REDIS =====
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
        SELECT assigned_to, store, total_lines, assigned_at
        FROM orders
        WHERE assigned = FALSE
        AND assigned_to IS NOT NULL
    """)
    rows = cur.fetchall()
    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(total_lines), 0)
        FROM orders
        WHERE assigned = FALSE
    """)
    total_orders, total_lines = cur.fetchone()
    conn.close()

    workers = {}
    for worker, store, lines, assigned_at in rows:
        if not worker:
            continue
        workers.setdefault(worker, {
            "lines": 0,
            "orders": 0,
            "stores": set(),
            "pending": False,
            "oldest": assigned_at
        })
        workers[worker]["lines"] += int(lines or 0)
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
        "total_lines": int(total_lines or 0)
    }

# ===== RESET SYSTEM =====
@app.route("/reset", methods=["POST"])
def reset_system():
    user = request.form.get("user") or request.headers.get("X-USER")

    # 🔐 доступ только для admin
    if user != "admin":
        return {"status": "error", "message": "Unauthorized"}, 403

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE orders")
        cur.execute("TRUNCATE TABLE store_locks")

        conn.commit()
        conn.close()

        # очистка Redis pending
        for key in r.keys("pending:*"):
            r.delete(key)

        return {"status": "ok"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

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
  <div id="summary" class="card"></div>
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

    if (!user) {
        alert("❌ Отменено");
        return;
    }

    if (!confirm("⚠️ Ты уверен что хочешь удалить ВСЕ заказы?")) return;

    let res = await fetch('/reset', {
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
        alert("✅ Система очищена");
    } else {
        alert("❌ Ошибка: " + data.message);
    }

    load();
    }
    async function load() {
      let res = await fetch('/dashboard_data');
      let data = await res.json();
      document.getElementById('summary').innerHTML =
        "<div class='big'>Remaining lines: " + data.total_lines + "</div>" +
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
          "⏱ Since: " + formatTime(u.oldest) + "</div>";
      }
      document.getElementById('workers').innerHTML = html;
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
      color: white;
      padding: 12px;
      font-weight: bold;
      text-align: center;
      margin-bottom: 12px;
      border-radius: 10px;
      animation: blink 1s infinite;
      font-size: 16px;
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
        if action == "upload" and user == "admin" and file:
            upload_orders(file)
        elif action == "confirm":
            confirm_orders(user)
            success = True
            orders = []
        else:
            orders, _, _ = assign_orders(user)
        if user and not orders:
            no_orders = True

    return render_template_string(
        HTML,
        orders=orders,
        user=user,
        no_orders=no_orders,
        success=success
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
