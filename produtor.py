"""Representa o estoQ publicando uma movimentação fictícia de estoque."""
import argparse
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from config import SERVIDOR, TOPICO


def quantidade_positiva(texto):
    numero = int(texto)
    if numero <= 0:
        raise argparse.ArgumentTypeError("A quantidade deve ser maior que zero.")
    return numero


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tipo", type=str.upper, choices=["ENTRADA", "SAIDA"])
    parser.add_argument("quantidade", type=quantidade_positiva)
    args = parser.parse_args()
    evento = {
        "id": str(uuid.uuid4()),
        "produto": "ARROZ-5KG",
        "tipo": args.tipo,
        "quantidade": args.quantidade,
        "data": datetime.now(timezone.utc).isoformat(),
    }
    produtor = Producer({"bootstrap.servers": SERVIDOR,
                         "enable.idempotence": True, "message.timeout.ms": 15000})
    falhas = []

    def confirmar(erro, mensagem):
        if erro:
            falhas.append(str(erro))
        else:
            print(f"Kafka confirmou: {mensagem.topic()} / "
                  f"partição {mensagem.partition()} / offset {mensagem.offset()}")

    produtor.produce(TOPICO, key=evento["produto"],
                     value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
                     on_delivery=confirmar)
    pendentes = produtor.flush(20)
    if pendentes or falhas:
        raise SystemExit(f"Falha ao enviar: {falhas or 'tempo de espera esgotado'}")
    print(json.dumps(evento, ensure_ascii=False))


if __name__ == "__main__":
    main()
