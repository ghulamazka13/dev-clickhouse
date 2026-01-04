import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def iso_ts(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_private_ip(rng):
    choice = rng.choice([10, 172, 192])
    if choice == 10:
        return f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    if choice == 172:
        return f"172.{rng.randint(16, 31)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    return f"192.168.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def build_base_event(rng):
    src_ip = random_private_ip(rng)
    dest_ip = random_private_ip(rng)
    src_port = rng.randint(1024, 65535)
    dest_port = rng.choice([22, 53, 80, 443, 3389, 8080, 8443])
    protocol = rng.choice(["tcp", "udp", "icmp"])
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": iso_ts(datetime.now(timezone.utc)),
        "src_ip": src_ip,
        "dest_ip": dest_ip,
        "src_port": src_port,
        "dest_port": dest_port,
        "protocol": protocol,
        "bytes": rng.randint(64, 200000),
        "packets": rng.randint(1, 2000),
        "sensor_name": f"sensor-{rng.randint(1, 5)}",
        "tags": rng.sample(["edge", "internal", "vpn", "prod", "dev"], k=2),
    }


def generate_zeek_event(rng):
    base = build_base_event(rng)
    event_type = rng.choice(["conn", "dns", "http"])
    severity = rng.choice(["low", "medium"])
    base.update(
        {
            "sensor_type": "zeek",
            "event_type": event_type,
            "severity": severity,
            "message": f"zeek {event_type} event from {base['src_ip']} to {base['dest_ip']}",
            "uid": f"C{rng.randint(100000, 999999)}",
            "conn_state": rng.choice(["S0", "S1", "SF", "REJ", "RSTO", "RSTR"]),
            "duration": round(rng.random() * 10, 3),
            "local_orig": rng.choice([True, False]),
            "local_resp": rng.choice([True, False]),
        }
    )
    return base


def generate_suricata_event(rng):
    base = build_base_event(rng)
    severity = rng.choices(["low", "medium", "high"], weights=[50, 35, 15])[0]
    signature = rng.choice(
        [
            "ET POLICY Suspicious inbound to MSSQL port",
            "ET SCAN Potential SSH scan",
            "GPL SQL injection attempt",
            "ET TROJAN Known malicious C2",
        ]
    )
    base.update(
        {
            "sensor_type": "suricata",
            "event_type": "alert",
            "severity": severity,
            "message": f"suricata alert: {signature}",
            "signature": signature,
            "signature_id": rng.randint(1000000, 2000000),
            "category": rng.choice(["attempted-recon", "trojan", "policy-violation"]),
            "alert_action": rng.choice(["allowed", "blocked"]),
        }
    )
    return base


def main():
    events_per_sec = max(env_int("EVENTS_PER_SEC", 10), 1)
    mix_zeek = env_int("MIX_ZEEK_PERCENT", 60)
    mix_suricata = env_int("MIX_SURICATA_PERCENT", 40)
    seed = env_int("SEED", 42)
    brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw.security_events")

    rng = random.Random(seed)
    total = mix_zeek + mix_suricata
    zeek_ratio = mix_zeek / total if total > 0 else 1.0

    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    try:
        while True:
            for _ in range(events_per_sec):
                if rng.random() < zeek_ratio:
                    event = generate_zeek_event(rng)
                else:
                    event = generate_suricata_event(rng)
                producer.send(topic, value=event)
                sent += 1
                if sent % 100 == 0:
                    producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
