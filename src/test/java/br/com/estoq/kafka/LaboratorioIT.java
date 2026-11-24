package br.com.estoq.kafka;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

class LaboratorioIT {
    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }

    @Test
    @Timeout(120)
    void publicaLeAlertaEReprocessaComKafkaReal() throws Exception {
        String topico = "estoq.teste." + UUID.randomUUID();
        Preparar.criarTopico(topico);
        try {
            Produtor.enviar(topico, "ENTRADA", 20);
            Produtor.enviar(topico, "SAIDA", 8);
            Produtor.enviar(topico, "SAIDA", 9);
            Produtor.enviar(topico, "ENTRADA", 10);
            for (int repeticao = 0; repeticao < 2; repeticao++) {
                String texto = ler(topico, false);
                assertEquals(4, texto.split("RECEBIDO", -1).length - 1);
                for (int saldo : new int[] {20, 12, 3, 13}) {
                    assertTrue(texto.contains("SALDO ARROZ-5KG: " + saldo + " unidades"), texto);
                }
                assertEquals(1, texto.split("ALERTA:", -1).length - 1);
                for (int offset = 0; offset < 4; offset++) {
                    assertTrue(texto.contains("partição=0 offset=" + offset));
                }
            }
            String leitura = ler(topico, true);
            assertEquals(4, leitura.split("RECEBIDO", -1).length - 1);
            assertFalse(leitura.contains("SALDO"));
        } finally {
            try (Admin admin = Admin.create(Config.propriedades())) {
                admin.deleteTopics(List.of(topico)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private String ler(String topico, boolean somenteLer) throws Exception {
        ByteArrayOutputStream dados = new ByteArrayOutputStream();
        try (PrintStream saida = new PrintStream(dados, true, StandardCharsets.UTF_8)) {
            Consumidor.ler(topico, 4, 20, somenteLer, saida);
        }
        return dados.toString(StandardCharsets.UTF_8);
    }
}
