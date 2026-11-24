package br.com.estoq.kafka;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumidor {
    public static void ler(String topico, int limite, int espera, boolean somenteLer, PrintStream saida)
            throws Exception {
        Properties propriedades = Config.propriedades();
        propriedades.put("key.deserializer", StringDeserializer.class.getName());
        propriedades.put("value.deserializer", StringDeserializer.class.getName());
        // Um grupo novo permite reconstruir o saldo desde o primeiro evento retido.
        propriedades.put("group.id", "estoq-lab-" + UUID.randomUUID());
        propriedades.put("auto.offset.reset", "earliest");
        propriedades.put("enable.auto.commit", "false");
        Estoque estoque = new Estoque();
        AtomicBoolean executando = new AtomicBoolean(true);

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propriedades)) {
            Thread encerrar = new Thread(() -> {
                executando.set(false);
                consumidor.wakeup();
            });
            Runtime.getRuntime().addShutdownHook(encerrar);
            consumidor.subscribe(List.of(topico));
            saida.println("Acompanhando " + topico + " desde o início. Ctrl+C para sair.");
            int recebidos = 0;
            long ultimaMensagem = System.nanoTime();
            try {
                while (executando.get() && (limite == 0 || recebidos < limite)) {
                    var registros = consumidor.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> registro : registros) {
                        Evento evento = Config.JSON.readValue(registro.value(), Evento.class);
                        saida.printf("%nRECEBIDO partição=%d offset=%d%n", registro.partition(), registro.offset());
                        saida.println(registro.value());
                        if (!somenteLer) {
                            Estoque.Resultado resultado = estoque.aplicar(evento);
                            saida.printf("SALDO %s: %d unidades%n", evento.produto(), resultado.saldo());
                            if (!resultado.novo()) {
                                saida.println("Evento repetido: saldo mantido.");
                            }
                            if (resultado.saldo() < 5) {
                                saida.println("ALERTA: estoque abaixo do mínimo de 5 unidades!");
                            }
                        }
                        recebidos++;
                        ultimaMensagem = System.nanoTime();
                        if (limite > 0 && recebidos >= limite) {
                            break;
                        }
                    }
                    if (espera > 0 && System.nanoTime() - ultimaMensagem > Duration.ofSeconds(espera).toNanos()) {
                        throw new IllegalStateException("Tempo de espera esgotado sem os eventos esperados.");
                    }
                }
            } catch (WakeupException erro) {
                if (executando.get()) {
                    throw erro;
                }
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(encerrar);
                } catch (IllegalStateException encerrandoJVM) {
                    // Ctrl+C já iniciou o encerramento da JVM.
                }
            }
        }
    }
}
