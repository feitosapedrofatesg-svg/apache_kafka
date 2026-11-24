import unittest
from estoque import Estoque


class TestEstoque(unittest.TestCase):
    def evento(self, id, tipo, quantidade):
        return {"id": id, "produto": "ARROZ-5KG", "tipo": tipo, "quantidade": quantidade}

    def test_entradas_saidas_e_releitura(self):
        eventos = [self.evento("1", "ENTRADA", 20), self.evento("2", "SAIDA", 8),
                   self.evento("3", "SAIDA", 9), self.evento("4", "ENTRADA", 10)]
        for _ in range(2):
            estoque = Estoque()
            saldos = [estoque.aplicar(e)[0] for e in eventos]
            self.assertEqual(saldos, [20, 12, 3, 13])

    def test_evento_repetido_nao_altera_saldo(self):
        estoque = Estoque()
        evento = self.evento("1", "ENTRADA", 20)
        self.assertEqual(estoque.aplicar(evento), (20, True))
        self.assertEqual(estoque.aplicar(evento), (20, False))

    def test_rejeita_eventos_invalidos_sem_alterar_saldo(self):
        estoque = Estoque()
        for tipo, quantidade in [("OUTRO", 5), ("ENTRADA", 0), ("SAIDA", -2), ("ENTRADA", True)]:
            with self.assertRaises(ValueError):
                estoque.aplicar(self.evento("1", tipo, quantidade))
        self.assertEqual(estoque.saldos, {})


if __name__ == "__main__":
    unittest.main()
