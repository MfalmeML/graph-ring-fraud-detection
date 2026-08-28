import json
import sys
from kafka import KafkaConsumer
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="shadow_decisions")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output", help="Write collected decisions to a JSON file")
    args = parser.parse_args()
    
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id="analysis-cli",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    
    print("=== Shadow Mode Analysis ===\n")
    decisions = []
    mismatch_count = 0
    ring_high_count = 0
    
    for _ in range(args.limit):
        msg = next(consumer, None)
        if not msg:
            break
        d = msg.value
        decisions.append(d)
        
        if d.get("shadow_decision") != d.get("production_decision"):
            mismatch_count += 1
        
        if d.get("ring_score", 0) > 0.7:
            ring_high_count += 1
    
    consumer.close()

    if args.output:
        with open(args.output, "w") as output_file:
            json.dump(decisions, output_file, indent=2)
        print(f"Saved decisions to {args.output}")
    
    if not decisions:
        print("No shadow decisions found.")
        return
    
    print(f"Total evaluated: {len(decisions)}")
    print(f"Mismatches: {mismatch_count} ({mismatch_count/len(decisions)*100:.2f}%)")
    print(f"Accounts with ring_score > 0.7: {ring_high_count}")
    print("\nSample decisions:")
    for d in decisions[:5]:
        print(f"  Account {d['account_id'][:8]}...: "
              f"Shadow={d['shadow_decision']}, "
              f"Production={d['production_decision']}, "
              f"RingScore={d['ring_score']:.3f}")

if __name__ == "__main__":
    main()