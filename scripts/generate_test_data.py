import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time

def generate_transaction(account_id, device_id, ip_address, merchant_id, card_id):
    return {
        "transaction_id": f"tx_{int(time.time())}_{random.randint(1000,9999)}",
        "account_id": account_id,
        "device_id": device_id,
        "ip_address": ip_address,
        "merchant_id": merchant_id,
        "card_id": card_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": round(random.uniform(10, 500), 2),
        "tabular_fraud_probability": random.uniform(0.01, 0.95)
    }

def generate_ring_data(num_accounts=5):
    shared_device = f"dev_ring_{random.randint(100,999)}"
    shared_ip = f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    shared_merchant = f"mer_ring_{random.randint(100,999)}"
    
    accounts = [f"acc_ring_{i}_{int(time.time())}" for i in range(num_accounts)]
    
    transactions = []
    for i, acc in enumerate(accounts):
        card = f"card_ring_{i}_{int(time.time())}"
        tx = generate_transaction(
            account_id=acc,
            device_id=shared_device,
            ip_address=shared_ip,
            merchant_id=shared_merchant,
            card_id=card
        )
        transactions.append(tx)
    
    return transactions

def generate_normal_transactions(n=10):
    transactions = []
    for i in range(n):
        tx = generate_transaction(
            account_id=f"acc_normal_{i}_{int(time.time())}",
            device_id=f"dev_normal_{i}_{int(time.time())}",
            ip_address=f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            merchant_id=f"mer_normal_{i}_{int(time.time())}",
            card_id=f"card_normal_{i}_{int(time.time())}"
        )
        transactions.append(tx)
    return transactions

if __name__ == "__main__":
    import sys
    
    producer = KafkaProducer(
        bootstrap_servers=sys.argv[1] if len(sys.argv) > 1 else "localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    print("Generating test data...")
    
    for _ in range(3):
        ring_txs = generate_ring_data(num_accounts=random.randint(3, 8))
        for tx in ring_txs:
            producer.send("transactions", tx)
            print(f"Sent ring transaction: {tx['transaction_id']}")
            time.sleep(0.1)
    
    normal_txs = generate_normal_transactions(20)
    for tx in normal_txs:
        producer.send("transactions", tx)
        print(f"Sent normal transaction: {tx['transaction_id']}")
        time.sleep(0.05)
    
    producer.flush()
    producer.close()
    print(f"Total transactions sent: {len(ring_txs) + len(normal_txs)}")