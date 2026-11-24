"""Cria o tópico de demonstração. Pode executar mais de uma vez."""
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from config import SERVIDOR, TOPICO


def main():
    admin = AdminClient({"bootstrap.servers": SERVIDOR})
    topico = NewTopic(TOPICO, num_partitions=1, replication_factor=1,
                     config={"cleanup.policy": "delete", "retention.ms": "604800000"})
    try:
        admin.create_topics([topico], request_timeout=30)[TOPICO].result()
        print(f"Tópico criado: {TOPICO}")
    except KafkaException as erro:
        if erro.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        print(f"Tópico já existe: {TOPICO}")
    print("Pronto: 1 partição, 1 réplica e retenção de 7 dias.")


if __name__ == "__main__":
    main()
