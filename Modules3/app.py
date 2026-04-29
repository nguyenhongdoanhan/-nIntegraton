from fastapi import FastAPI
from sqlalchemy import create_engine, text
import pandas as pd
from pydantic import BaseModel
from typing import List
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔹 Model trả về
class ReportItem(BaseModel):
    customer_id: int
    amount: float

# 🔹 Kết nối MySQL
mysql_engine = create_engine(
    "mysql+pymysql://root:@localhost:3307/testdb"
)

# 🔹 Test kết nối
def check_connection():
    try:
        with mysql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ MySQL connected")
    except Exception as e:
        print("❌ MySQL connection error:", e)

check_connection()


# 🔥 API report
@app.get("/api/report", response_model=List[ReportItem])
def get_report():
    try:
        query = """
        SELECT 
            user_id AS customer_id,
            total_price AS amount
        FROM orders
        """

        df = pd.read_sql(query, mysql_engine)

        print("Raw rows:", len(df))

        # ❗ Nếu không có dữ liệu
        if df.empty:
            return []

        # 🔹 convert Decimal → float
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        # 🔥 group dữ liệu
        df_grouped = df.groupby("customer_id", as_index=False)["amount"].sum()

        print("Customers:", len(df_grouped))
        print(df_grouped.head())  # 🔥 debug thêm

        # ❗ Nếu group ra rỗng
        if df_grouped.empty:
            return []

        # 🔥 FIX CHÍNH Ở ĐÂY (quan trọng)
        return [
            ReportItem(
                customer_id=int(row["customer_id"]),
                amount=float(row["amount"])
            )
            for _, row in df_grouped.iterrows()
        ]

    except Exception as e:
        print("❌ ERROR:", e)
        return []
        