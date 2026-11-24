"""Projeção simples do estoque em memória, reconstruída pela leitura dos eventos."""


class Estoque:
    def __init__(self):
        self.saldos = {}
        self.processados = set()

    def aplicar(self, evento):
        identificador = evento["id"]
        produto = evento["produto"]
        tipo = evento["tipo"]
        quantidade = evento["quantidade"]
        if not isinstance(identificador, str) or not identificador:
            raise ValueError("ID inválido")
        if not isinstance(produto, str) or not produto:
            raise ValueError("Produto inválido")
        if tipo not in ("ENTRADA", "SAIDA"):
            raise ValueError("Tipo de movimentação inválido")
        if type(quantidade) is not int or quantidade <= 0:
            raise ValueError("Quantidade deve ser um inteiro positivo")
        if identificador in self.processados:
            return self.saldos.get(produto, 0), False
        saldo = self.saldos.get(produto, 0)
        saldo += quantidade if tipo == "ENTRADA" else -quantidade
        # O consumidor apenas projeta eventos já registrados na origem.
        # Validações de uma saída no sistema real pertencem à transação do estoQ.
        self.saldos[produto] = saldo
        self.processados.add(identificador)
        return saldo, True
