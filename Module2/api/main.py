from fastapi import FastAPI
import mysql.connector
import pika
import json
import time

app = FastAPI()


def mysql_conn(retries=10, delay=2):
    last_exc = None
    for _ in range(retries):
        try:
            return mysql.connector.connect(
                host="mysql",
                user="root",
                password="root",
                database="noah"
            )
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc

@app.get("/")
def home():
    return {"message": "API running"}

@app.post("/api/orders")
def create_order(order: dict):
    user_id = order["user_id"]
    product_id = order["product_id"]
    quantity = order["quantity"]

    if quantity <= 0:
        return {"error": "Quantity must be > 0"}

    conn = mysql_conn()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO orders (user_id, product_id, quantity, status) VALUES (%s,%s,%s,'PENDING')",
        (user_id, product_id, quantity)
    )
    conn.commit()

    order_id = cursor.lastrowid

    # ensure rabbitmq is reachable (retry)
    for _ in range(10):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq')
            )
            channel = connection.channel()
            channel.queue_declare(queue='order_queue')
            break
        except Exception:
            time.sleep(2)
    else:
        return {"error": "Message broker unavailable"}

    message = {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity
    }

    channel.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=json.dumps(message)
    )

    connection.close()

    return {"message": "Order received", "order_id": order_id}
