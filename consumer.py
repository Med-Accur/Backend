from kafka import KafkaConsumer
import redis
import json

# Redis synchrone
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# Kafka Consumer (écoute plusieurs topics)
consumer = KafkaConsumer(
    "orders_events", "contact_events",  # ajouter ici toutes les tables que tu veux écouter
    bootstrap_servers="localhost:9092",
    group_id="cache-invalidator",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("📡 En attente des events Kafka...")
print("Topics écoutés:", consumer.subscription())
for msg in consumer:
    event = msg.value
    table = event.get("table")
    action = event.get("type")
    record = event.get("record")

    print(f" Event reçu: {action} sur {table} → {record}")

    if record and action in ("UPDATE", "DELETE"):
        # Supprime la clé Redis correspondante
        print(f"[CACHE] Invalidation du cache pour la table '{table}'")
        cache_key = f"table_cache:{table}"
        redis_client.delete(cache_key)
        print(f" Cache supprimé dans Redis: {cache_key}")
