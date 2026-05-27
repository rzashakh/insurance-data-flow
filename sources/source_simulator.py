import psycopg2, random, time, uuid, os, logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("simulator")

product_lines = ["auto", "home", "health", "travel"]
regions = ["baku", "ganja", "sumqayit", "mingachevir"]


def connect():
    while True:
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME", "insurance"),
                user=os.getenv("DB_USER", "insurance_user"),
                password=os.getenv("DB_PASSWORD", "root"),
                host=os.getenv("DB_HOST", "data-lab-cluster-rw"),
            )
            conn.autocommit = True
            log.info("Connected to database")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready, retrying in 5s: %s", e)
            time.sleep(5)


conn = connect()
cur = conn.cursor()

while True:
    try:
        policy_id = str(uuid.uuid4())
        product = random.choice(product_lines)
        region = random.choice(regions)
        premium = round(random.uniform(200, 2000), 2)

        cur.execute(
            "INSERT INTO policies (id, product_line, region, premium, start_date) "
            "VALUES (%s, %s, %s, %s, %s)",
            (policy_id, product, region, premium, datetime.now()),
        )
        log.info("policy  id=%s product=%s region=%s premium=%.2f", policy_id, product, region, premium)

        if random.random() < 0.4:
            claim_id = str(uuid.uuid4())
            amount = round(random.uniform(100, 15000), 2)
            cur.execute(
                "INSERT INTO claims (id, policy_id, amount, status, event_ts) "
                "VALUES (%s, %s, %s, %s, %s)",
                (claim_id, policy_id, amount, "open", datetime.now()),
            )
            log.info("claim   id=%s policy=%s amount=%.2f", claim_id, policy_id, amount)

        if random.random() < 0.2:
            new_status = random.choice(["processing", "closed"])
            cur.execute(
                "UPDATE claims SET status = %s, event_ts = %s "
                "WHERE id = (SELECT id FROM claims ORDER BY random() LIMIT 1)",
                (new_status, datetime.now()),
            )
            log.info("update  claim status -> %s", new_status)

    except psycopg2.OperationalError as e:
        log.error("Lost DB connection, reconnecting: %s", e)
        conn = connect()
        cur = conn.cursor()

    time.sleep(10)