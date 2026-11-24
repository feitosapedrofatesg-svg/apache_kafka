package br.com.estoq.kafka;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EstoqueTest {
    private Evento evento(String id, String tipo, int quantidade) {
        return new Evento(id, "ARROZ-5KG", tipo, quantidade, "2026-11-24T12:00:00Z");
    }

    @Test
    void calculaSaldoEReconstruiPeloHistorico() {
        var eventos = new Evento[] {evento("1", "ENTRADA", 20), evento("2", "SAIDA", 8),
                evento("3", "SAIDA", 9), evento("4", "ENTRADA", 10)};
        for (int repeticao = 0; repeticao < 2; repeticao++) {
            Estoque estoque = new Estoque();
            int[] esperados = {20, 12, 3, 13};
            for (int i = 0; i < eventos.length; i++) {
                assertEquals(esperados[i], estoque.aplicar(eventos[i]).saldo());
            }
        }
    }

    @Test
    void mesmoIdNaoAlteraSaldoOutraVez() {
        Estoque estoque = new Estoque();
        Evento evento = evento("1", "ENTRADA", 20);
        assertEquals(new Estoque.Resultado(20, true), estoque.aplicar(evento));
        assertEquals(new Estoque.Resultado(20, false), estoque.aplicar(evento));
    }

    @Test
    void rejeitaMovimentacaoInvalida() {
        assertThrows(IllegalArgumentException.class, () -> evento("1", "OUTRO", 5));
        assertThrows(IllegalArgumentException.class, () -> evento("1", "ENTRADA", 0));
        assertThrows(IllegalArgumentException.class, () -> evento("1", "SAIDA", -2));
        assertThrows(IllegalArgumentException.class, () -> evento("", "ENTRADA", 5));
    }
}
