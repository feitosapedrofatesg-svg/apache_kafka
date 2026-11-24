package br.com.estoq.kafka;

import java.time.Instant;
import java.util.UUID;

// Um record é uma classe simples para guardar os dados da movimentação.
public record Evento(String id, String produto, String tipo, int quantidade, String data) {
    public Evento {
        if (id == null || id.isBlank() || produto == null || produto.isBlank()) {
            throw new IllegalArgumentException("ID e produto são obrigatórios.");
        }
        if (!"ENTRADA".equals(tipo) && !"SAIDA".equals(tipo)) {
            throw new IllegalArgumentException("Use ENTRADA ou SAIDA.");
        }
        if (quantidade <= 0) {
            throw new IllegalArgumentException("A quantidade deve ser maior que zero.");
        }
    }

    public static Evento criar(String tipo, int quantidade) {
        return new Evento(UUID.randomUUID().toString(), "ARROZ-5KG", tipo,
                quantidade, Instant.now().toString());
    }
}
