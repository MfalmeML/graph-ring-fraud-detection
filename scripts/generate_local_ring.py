#!/usr/bin/env python3
import sys
import time
import random
from datetime import datetime

sys.path.insert(0, '.')

try:
    from src.ingestion.local_consumer import produce_transaction
except ImportError:
    from src.ingestion.kafka_simulator import produce_transaction

def generate_ring(num_accounts=5):
    shared_device = f"dev_ring_{random.randint(100,999)}"
    shared_ip = f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    shared_merchant = f"mer_ring_{random.randint(100,999)}"
    
    accounts = []
    for i in range(num_accounts):
        acc = f"acc_ring_{i}_{int(time.time())}"
        accounts.append(acc)
        card = f"card_ring_{i}_{int(time.time())}"
        produce_transaction(
            account_id=acc,
            device_id=shared_device,
            ip_address=shared_ip,
            merchant_id=shared_merchant,
            card_id=card,
            amount=random.uniform(50, 300)
        )
        time.sleep(0.1)
    
    print(f"Generated ring with {num_accounts} accounts")
    print(f"Shared device: {shared_device}")
    print(f"Shared IP: {shared_ip}")
    print(f"Shared merchant: {shared_merchant}")
    print(f"Accounts: {', '.join(accounts)}")
    return accounts

def generate_normal_transactions(n=10):
    for i in range(n):
        produce_transaction(
            account_id=f"acc_normal_{i}_{int(time.time())}",
            device_id=f"dev_normal_{i}_{int(time.time())}",
            ip_address=f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            merchant_id=f"mer_normal_{i}_{int(time.time())}",
            card_id=f"card_normal_{i}_{int(time.time())}",
            amount=random.uniform(10, 500)
        )
        time.sleep(0.05)
    print(f"Generated {n} normal transactions")

if __name__ == "__main__":
    print("=== Generating Test Data ===")
    
    # Generate 3 rings
    for i in range(3):
        generate_ring(num_accounts=random.randint(3, 6))
        time.sleep(0.5)
    
    # Generate normal transactions
    generate_normal_transactions(20)
    
    print("\nTest data generation complete.")
    print("Check ring_score API for accounts in rings.")
    print("Check investigator API for candidate rings.")