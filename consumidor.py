"""Lê eventos reais do Kafka e calcula o saldo do exemplo inspirado no estoQ."""
import argparse
import json
import time
import uuid
from confluent_kafka import Consumer
from config import SERVIDOR, TOPICO
from estoque import Estoque


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--somente-ler", action="store_true", help="Exibe apenas os eventos")
    parser.add_argument("--limite", type=int, default=0, help="Encerra após N mensagens (0 = contínuo)")
    parser.add_argument("--espera", type=float, default=0, help="Limite de espera sem eventos em segundos")
    args = parser.parse_args()
    if args.limite < 0 or args.espera < 0:
        parser.error("Limite e espera não podem ser negativos")
    consumidor = Consumer({
        "bootstrap.servers": SERVIDOR,
        # Novo grupo em cada execução para demonstrar a releitura desde o início.
        "group.id": f"estoq-lab-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    estoque = Estoque()
    contador = 0
    ultima_mensagem = time.monotonic()
    consumidor.subscribe([TOPICO])
    print(f"Acompanhando {TOPICO} desde o início. Ctrl+C para sair.", flush=True)
    try:
        while not args.limite or contador < args.limite:
            mensagem = consumidor.poll(1)
            if mensagem is None:
                if args.espera and time.monotonic() - ultima_mensagem > args.espera:
                    raise SystemExit("Tempo de espera esgotado sem receber os eventos esperados.")
                continue
            if mensagem.error():
                raise SystemExit(str(mensagem.error()))
            ultima_mensagem = time.monotonic()
            evento = json.loads(mensagem.value().decode("utf-8"))
            print(f"\nRECEBIDO partição={mensagem.partition()} offset={mensagem.offset()}")
            print(json.dumps(evento, ensure_ascii=False))
            if not args.somente_ler:
                saldo, novo = estoque.aplicar(evento)
                print(f"SALDO {evento['produto']}: {saldo} unidades")
                if not novo:
                    print("Evento repetido: saldo mantido.")
                if saldo < 5:
                    print("ALERTA: estoque abaixo do mínimo de 5 unidades!")
            contador += 1
    except KeyboardInterrupt:
        print("\nConsumidor encerrado. Os eventos continuam no Kafka.")
    finally:
        consumidor.close()


if __name__ == "__main__":
    main()
