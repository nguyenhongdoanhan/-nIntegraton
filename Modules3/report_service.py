from fastapi import FastAPI
from sqlalchemy import create_engine, text
import pandas as pd
import time

app = FastAPI()

# 🔹 Kết nối MySQL
mysql_engine = create_engine(
    "mysql+pymysql://root:@localhost:3306/testdb"
)

# 🔹 Kết nối PostgreSQL
postgres_engine = create_engine(
    "postgresql://postgres:password@localhost:5432/testdb"
)

# 🔹 Retry DB
def wait_for_db(engine, name):
    for i in range(5):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✅ {name} connected")
            return
        except Exception as e:
            print(f"⏳ Waiting for {name}...", e)
            time.sleep(3)
    print(f"❌ {name} failed")

wait_for_db(mysql_engine, "MySQL")
wait_for_db(postgres_engine, "PostgreSQL")


@app.get("/api/report")
def get_report():
    try:
        # 🔹 1. Lấy orders
        orders = pd.read_sql("""
            SELECT id AS order_id, user_id, total_price
            FROM orders
        """, mysql_engine)

        print("Orders rows:", len(orders))

        if orders.empty:
            return []

        # 🔹 2. Lấy payments (có thể rỗng)
        try:
            payments = pd.read_sql("""
                SELECT order_id, amount
                FROM payments
            """, postgres_engine)
        except Exception as e:
            print("⚠️ Không đọc được payments:", e)
            payments = pd.DataFrame(columns=["order_id", "amount"])

        print("Payments rows:", len(payments))

        # 🔹 3. Ép kiểu an toàn
        orders["order_id"] = pd.to_numeric(orders["order_id"], errors="coerce")

        if not payments.empty:
            payments["order_id"] = pd.to_numeric(payments["order_id"], errors="coerce")

        # 🔥 4. MERGE (KHÔNG mất dữ liệu)
        df = pd.merge(orders, payments, on="order_id", how="left")

        print("Merged rows:", len(df))

        # 🔥 5. FALLBACK nếu payment null
        df["amount"] = df["amount"].fillna(df["total_price"])

        # 🔹 6. GROUP BY
        result = (
            df.groupby("user_id")["amount"]
            .sum()
            .reset_index()
        )

        result.columns = ["customer_id", "amount"]

        print("Final rows:", len(result))

        return result.to_dict(orient="records")

    except Exception as e:
        print("❌ ERROR:", e)
        return []