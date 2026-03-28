"""
Apple Retail POS Transaction Producer
Simulates real-time sales events from Apple Store locations worldwide.
Sends JSON events to a Kafka topic: 'retail-transactions'
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ─── Kafka Configuration ───────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC = "retail-transactions"
EVENTS_PER_SECOND = 2          # average throughput (adjust for load testing)
FRAUD_INJECT_RATE = 0.03       # 3% of transactions are suspicious

# ─── Apple Store Locations ─────────────────────────────────────────────────────

STORES = [
    {"store_id": "R001", "name": "Apple Fifth Avenue",         "city": "New York",      "country": "US",  "currency": "USD", "region": "AMER"},
    {"store_id": "R002", "name": "Apple Infinite Loop",        "city": "Cupertino",     "country": "US",  "currency": "USD", "region": "AMER"},
    {"store_id": "R003", "name": "Apple Michigan Avenue",      "city": "Chicago",       "country": "US",  "currency": "USD", "region": "AMER"},
    {"store_id": "R004", "name": "Apple The Grove",            "city": "Los Angeles",   "country": "US",  "currency": "USD", "region": "AMER"},
    {"store_id": "R005", "name": "Apple Regent Street",        "city": "London",        "country": "GB",  "currency": "GBP", "region": "EMEA"},
    {"store_id": "R006", "name": "Apple Champs-Élysées",       "city": "Paris",         "country": "FR",  "currency": "EUR", "region": "EMEA"},
    {"store_id": "R007", "name": "Apple Rosenthaler Strasse",  "city": "Berlin",        "country": "DE",  "currency": "EUR", "region": "EMEA"},
    {"store_id": "R008", "name": "Apple Dubai Mall",           "city": "Dubai",         "country": "AE",  "currency": "AED", "region": "EMEA"},
    {"store_id": "R009", "name": "Apple Orchard Road",         "city": "Singapore",     "country": "SG",  "currency": "SGD", "region": "APAC"},
    {"store_id": "R010", "name": "Apple Sanlitun",             "city": "Beijing",       "country": "CN",  "currency": "CNY", "region": "APAC"},
    {"store_id": "R011", "name": "Apple Omotesando",           "city": "Tokyo",         "country": "JP",  "currency": "JPY", "region": "APAC"},
    {"store_id": "R012", "name": "Apple Saket",                "city": "New Delhi",     "country": "IN",  "currency": "INR", "region": "APAC"},
    {"store_id": "R013", "name": "Apple BKC",                  "city": "Mumbai",        "country": "IN",  "currency": "INR", "region": "APAC"},
    {"store_id": "R014", "name": "Apple Sydney",               "city": "Sydney",        "country": "AU",  "currency": "AUD", "region": "APAC"},
    {"store_id": "R015", "name": "Apple Covent Garden",        "city": "London",        "country": "GB",  "currency": "GBP", "region": "EMEA"},
]

# ─── Product Catalog (USD base prices) ─────────────────────────────────────────

PRODUCTS = [
    # iPhone 16 line
    {"sku": "IPHONE16-128",    "name": "iPhone 16 128GB",             "category": "iPhone",    "base_price_usd": 799,   "weight": 18},
    {"sku": "IPHONE16-256",    "name": "iPhone 16 256GB",             "category": "iPhone",    "base_price_usd": 899,   "weight": 15},
    {"sku": "IPHONE16P-256",   "name": "iPhone 16 Pro 256GB",         "category": "iPhone",    "base_price_usd": 999,   "weight": 14},
    {"sku": "IPHONE16P-512",   "name": "iPhone 16 Pro 512GB",         "category": "iPhone",    "base_price_usd": 1199,  "weight": 10},
    {"sku": "IPHONE16PM-256",  "name": "iPhone 16 Pro Max 256GB",     "category": "iPhone",    "base_price_usd": 1199,  "weight": 10},
    {"sku": "IPHONE16PM-1TB",  "name": "iPhone 16 Pro Max 1TB",       "category": "iPhone",    "base_price_usd": 1599,  "weight": 5},
    # Mac
    {"sku": "MBA-M3-8-256",    "name": "MacBook Air 13 M3 8GB",       "category": "Mac",       "base_price_usd": 1099,  "weight": 8},
    {"sku": "MBA-M3-16-512",   "name": "MacBook Air 15 M3 16GB",      "category": "Mac",       "base_price_usd": 1499,  "weight": 6},
    {"sku": "MBP-M4-16-512",   "name": "MacBook Pro 14 M4 16GB",      "category": "Mac",       "base_price_usd": 1999,  "weight": 5},
    {"sku": "MBP-M4P-24-512",  "name": "MacBook Pro 16 M4 Pro 24GB",  "category": "Mac",       "base_price_usd": 2499,  "weight": 3},
    {"sku": "IMAC-M4-24",      "name": "iMac 24 M4 16GB",             "category": "Mac",       "base_price_usd": 1699,  "weight": 2},
    {"sku": "MACMINI-M4",      "name": "Mac mini M4",                 "category": "Mac",       "base_price_usd": 599,   "weight": 4},
    # iPad
    {"sku": "IPAD-A16-128",    "name": "iPad 10th Gen 128GB",         "category": "iPad",      "base_price_usd": 449,   "weight": 9},
    {"sku": "IPADAIR-M2-128",  "name": "iPad Air 11 M2 128GB",        "category": "iPad",      "base_price_usd": 599,   "weight": 7},
    {"sku": "IPADPRO-M4-256",  "name": "iPad Pro 11 M4 256GB",        "category": "iPad",      "base_price_usd": 999,   "weight": 5},
    {"sku": "IPADPRO13-M4-1T", "name": "iPad Pro 13 M4 1TB",          "category": "iPad",      "base_price_usd": 1899,  "weight": 2},
    # Apple Watch
    {"sku": "AW-S10-40",       "name": "Apple Watch Series 10 40mm",  "category": "Watch",     "base_price_usd": 399,   "weight": 12},
    {"sku": "AW-S10-44",       "name": "Apple Watch Series 10 44mm",  "category": "Watch",     "base_price_usd": 429,   "weight": 10},
    {"sku": "AWU2-49",         "name": "Apple Watch Ultra 2 49mm",    "category": "Watch",     "base_price_usd": 799,   "weight": 4},
    # AirPods
    {"sku": "AIRPODS4-ANC",    "name": "AirPods 4 ANC",               "category": "AirPods",   "base_price_usd": 179,   "weight": 20},
    {"sku": "AIRPODSPRO2",     "name": "AirPods Pro 2",               "category": "AirPods",   "base_price_usd": 249,   "weight": 16},
    {"sku": "AIRPODSMAX-USB",  "name": "AirPods Max USB-C",           "category": "AirPods",   "base_price_usd": 549,   "weight": 6},
    # Accessories
    {"sku": "APPLETV4K",       "name": "Apple TV 4K 128GB",           "category": "Accessory", "base_price_usd": 129,   "weight": 10},
    {"sku": "HOMEPODMINI",     "name": "HomePod mini",                "category": "Accessory", "base_price_usd": 99,    "weight": 8},
    {"sku": "MAGSAFE-CHARGER", "name": "MagSafe Charger 2m",          "category": "Accessory", "base_price_usd": 45,    "weight": 12},
    {"sku": "APPLECARE-IP16",  "name": "AppleCare+ iPhone 16",        "category": "Service",   "base_price_usd": 199,   "weight": 14},
    {"sku": "APPLECARE-MBP",   "name": "AppleCare+ MacBook Pro",      "category": "Service",   "base_price_usd": 299,   "weight": 8},
]

# ─── Currency Multipliers (approximate, for realism) ───────────────────────────

FX_RATES = {
    "USD": 1.00,
    "GBP": 0.79,
    "EUR": 0.92,
    "AED": 3.67,
    "SGD": 1.35,
    "CNY": 7.24,
    "JPY": 149.5,
    "INR": 83.2,
    "AUD": 1.53,
}

PAYMENT_METHODS = ["Apple Pay", "Credit Card", "Debit Card", "Gift Card", "Cash", "Financing"]
PAYMENT_WEIGHTS  = [35, 30, 15, 10, 5, 5]   # Apple Pay dominates

CUSTOMER_TYPES = ["walk-in", "online-pickup", "business", "education", "employee"]
CUSTOMER_WEIGHTS = [55, 20, 10, 10, 5]

# ─── Helper Functions ──────────────────────────────────────────────────────────

def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]

def pick_product():
    weights = [p["weight"] for p in PRODUCTS]
    return weighted_choice(PRODUCTS, weights)

def compute_price(product, store, fraudulent=False):
    """
    Converts USD base price to local currency.
    Fraudulent transactions use extreme prices to trip fraud rules.
    """
    base = product["base_price_usd"]
    rate = FX_RATES.get(store["currency"], 1.0)
    normal_price = round(base * rate * random.uniform(0.98, 1.02), 2)

    if fraudulent:
        fraud_type = random.choice(["spike", "zero", "negative"])
        if fraud_type == "spike":
            return round(normal_price * random.uniform(8, 20), 2)   # absurdly high
        elif fraud_type == "zero":
            return round(random.uniform(0.01, 0.99), 2)             # near-zero
        else:
            return round(-abs(normal_price), 2)                     # negative price
    return normal_price

def generate_transaction():
    store   = random.choice(STORES)
    product = pick_product()
    is_fraud = random.random() < FRAUD_INJECT_RATE
    price   = compute_price(product, store, fraudulent=is_fraud)
    qty     = 1 if product["category"] not in ("Accessory", "Service") else random.randint(1, 3)

    event = {
        "transaction_id":  str(uuid.uuid4()),
        "event_time":      datetime.now(timezone.utc).isoformat(),
        "store_id":        store["store_id"],
        "store_name":      store["name"],
        "city":            store["city"],
        "country":         store["country"],
        "region":          store["region"],
        "currency":        store["currency"],
        "sku":             product["sku"],
        "product_name":    product["name"],
        "category":        product["category"],
        "unit_price":      price,
        "quantity":        qty,
        "total_amount":    round(price * qty, 2),
        "payment_method":  weighted_choice(PAYMENT_METHODS, PAYMENT_WEIGHTS),
        "customer_type":   weighted_choice(CUSTOMER_TYPES, CUSTOMER_WEIGHTS),
        "is_fraud_flag":   is_fraud,         # ground truth — Spark should detect independently
        "producer_version": "1.0.0",
    }
    return event

# ─── Kafka Producer ────────────────────────────────────────────────────────────

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",                     # wait for full replication acknowledgement
        retries=3,
        compression_type="gzip",        # reduces network I/O
    )

def on_send_success(record_metadata):
    print(
        f"  [OK] partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )

def on_send_error(excp):
    print(f"  [ERR] {excp}")

# ─── Main Loop ─────────────────────────────────────────────────────────────────

def main():
    print(f"Starting Apple POS producer → topic: '{TOPIC}' @ {KAFKA_BROKER}")
    print(f"Rate: ~{EVENTS_PER_SECOND} events/sec | Fraud injection: {FRAUD_INJECT_RATE*100:.0f}%\n")

    producer = create_producer()
    sent = 0
    sleep_interval = 1.0 / EVENTS_PER_SECOND

    try:
        while True:
            txn = generate_transaction()
            key = txn["store_id"]           # partition by store for ordered processing

            future = producer.send(TOPIC, key=key, value=txn)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            sent += 1
            fraud_marker = " ⚠ FRAUD" if txn["is_fraud_flag"] else ""
            print(
                f"[{sent:>6}] {txn['store_id']} | {txn['country']} | "
                f"{txn['product_name']:<35} | "
                f"{txn['currency']} {txn['total_amount']:>10.2f} | "
                f"{txn['payment_method']:<12}{fraud_marker}"
            )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {sent}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed cleanly.")

if __name__ == "__main__":
    main()
