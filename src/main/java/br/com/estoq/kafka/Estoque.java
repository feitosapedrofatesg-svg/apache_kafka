package br.com.estoq.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Estoque {
    private final Map<String, Integer> saldos = new HashMap<>();
    private final Set<String> processados = new HashSet<>();

    public record Resultado(int saldo, boolean novo) {}

    public Resultado aplicar(Evento evento) {
        int saldo = saldos.getOrDefault(evento.produto(), 0);
        if (processados.contains(evento.id())) {
            return new Resultado(saldo, false);
        }
        int variacao = evento.tipo().equals("ENTRADA") ? evento.quantidade() : -evento.quantidade();
        saldo = Math.addExact(saldo, variacao);
        // Projeção dos eventos. A validação da saída pertence ao sistema de origem.
        saldos.put(evento.produto(), saldo);
        processados.add(evento.id());
        return new Resultado(saldo, true);
    }
}
