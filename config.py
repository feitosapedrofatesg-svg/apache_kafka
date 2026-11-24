"""Endereços compartilhados pelos programas do laboratório."""
import os

SERVIDOR = os.getenv("KAFKA_SERVIDOR", "kafka:9092")
TOPICO = os.getenv("KAFKA_TOPICO", "estoq.movimentacoes")
