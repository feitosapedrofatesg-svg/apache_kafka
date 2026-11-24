"""Teste com Kafka real em tópico temporário, sem alterar a demonstração."""
import os
import subprocess
import sys
import unittest
import uuid
from confluent_kafka.admin import AdminClient
from config import SERVIDOR


class TestKafkaReal(unittest.TestCase):
    def test_publicacao_leitura_alerta_e_reprocessamento(self):
        topico = f"estoq.teste.{uuid.uuid4().hex}"
        ambiente = dict(os.environ, KAFKA_TOPICO=topico)

        def executar(*argumentos):
            return subprocess.run([sys.executable, *argumentos], env=ambiente,
                                  text=True, capture_output=True, check=True, timeout=60).stdout

        try:
            executar("preparar.py")
            for tipo, quantidade in [("ENTRADA", "20"), ("SAIDA", "8"),
                                     ("SAIDA", "9"), ("ENTRADA", "10")]:
                saida = executar("produtor.py", tipo, quantidade)
                self.assertIn("Kafka confirmou", saida)
            for _ in range(2):
                saida = executar("consumidor.py", "--limite", "4", "--espera", "20")
                self.assertEqual(saida.count("RECEBIDO"), 4)
                for saldo in [20, 12, 3, 13]:
                    self.assertIn(f"SALDO ARROZ-5KG: {saldo} unidades", saida)
                self.assertEqual(saida.count("ALERTA:"), 1)
                for offset in range(4):
                    self.assertIn(f"partição=0 offset={offset}", saida)
            leitura = executar("consumidor.py", "--somente-ler", "--limite", "4", "--espera", "20")
            self.assertEqual(leitura.count("RECEBIDO"), 4)
            self.assertNotIn("SALDO", leitura)
        finally:
            admin = AdminClient({"bootstrap.servers": SERVIDOR})
            admin.delete_topics([topico])[topico].result()


if __name__ == "__main__":
    unittest.main()
