import pika
import json
import time
import psycopg2
import mysql.connector


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


def postgres_conn(retries=10, delay=2):
    last_exc = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host="postgres",
                database="finance",
                user="postgres",
                password="postgres"
            )
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc

def callback(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]

    print(f"Processing order {order_id}")
    time.sleep(2)

    pg = postgres_conn()
    cur_pg = pg.cursor()
    cur_pg.execute(
        "INSERT INTO transactions (order_id, amount) VALUES (%s, %s)",
        (order_id, 100)
    )
    pg.commit()

    my = mysql_conn()
    cur_my = my.cursor()
    cur_my.execute(
        "UPDATE orders SET status='COMPLETED' WHERE id=%s",
        (order_id,)
    )
    my.commit()

    print(f"Done order {order_id}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def rabbit_connect(retries=10, delay=2):
    last_exc = None
    for _ in range(retries):
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq')
            )
            ch = conn.channel()
            ch.queue_declare(queue='order_queue')
            return conn, ch
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc


conn, channel = rabbit_connect()
channel.basic_consume(queue='order_queue', on_message_callback=callback)

print("Waiting for orders...")
channel.start_consuming()
