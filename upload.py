import pandas as pd
import psycopg2
import os

DB_URL = os.environ.get("DATABASE_URL")

def upload_excel(file):
    df = pd.read_excel(file)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    for _, row in df.iterrows():
        try:
            cur.execute(
                """
                INSERT INTO orders (order_id, store, qty, susr3, ref)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
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
            print("ERROR ROW:", row, e)

    conn.commit()
    conn.close()

    print("✅ UPLOAD DONE")

# запуск
upload_excel("orders.xlsx")
