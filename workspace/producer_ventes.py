from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime, timezone

# Callback de confirmation
def delivery_report(err, msg):
    if err is not None:
        print(f"⚠️ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# Configuration Kafka
conf = {
    "bootstrap.servers": "localhost:9092",  # Windows + Docker : localhost
    "acks": "all",
    "retries": 3,
    "linger.ms": 100
}

producer = Producer(conf)

# Données simulées
pays = ["Tunisie", "France", "Allemagne"]
segments = ["Retail", "Online", "B2B"]
produits = ["PC", "Téléphone", "Tablette"]

try:
    while True:
        vente = {
            "vente_id": random.randint(1000, 9999),
            "timestamp": datetime.now(timezone.utc).isoformat(),  # UTC aware
            "pays": random.choice(pays),
            "segment": random.choice(segments),
            "produit": random.choice(produits),
            "quantite": random.randint(1, 5),
            "prix_unitaire": round(random.uniform(100, 2000), 2)
        }

        producer.produce(
            topic="ventes_stream",
            value=json.dumps(vente),
            on_delivery=delivery_report
        )

        producer.poll(0)  # déclenche les callbacks
        print("🛒 Vente envoyée :", vente)
        time.sleep(2)

except KeyboardInterrupt:
    print("\nArrêt du producteur...")
finally:
    producer.flush()  # s'assurer que tous les messages sont envoyés
