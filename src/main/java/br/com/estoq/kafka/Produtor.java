package br.com.estoq.kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Produtor {
    public static void enviar(String topico, String tipo, int quantidade) throws Exception {
        Evento evento = Evento.criar(tipo, quantidade);
        String json = Config.JSON.writeValueAsString(evento);
        Properties propriedades = Config.propriedades();
        propriedades.put("key.serializer", StringSerializer.class.getName());
        propriedades.put("value.serializer", StringSerializer.class.getName());
        propriedades.put("enable.idempotence", "true");
        propriedades.put("delivery.timeout.ms", "15000");
        propriedades.put("request.timeout.ms", "5000");
        propriedades.put("max.block.ms", "15000");

        try (KafkaProducer<String, String> produtor = new KafkaProducer<>(propriedades)) {
            ProducerRecord<String, String> registro = new ProducerRecord<>(topico, evento.produto(), json);
            // Esperamos a confirmação do Kafka antes de dizer que o envio funcionou.
            RecordMetadata resposta = produtor.send(registro).get(20, TimeUnit.SECONDS);
            System.out.printf("Kafka confirmou: %s / partição %d / offset %d%n",
                    resposta.topic(), resposta.partition(), resposta.offset());
            System.out.println(json);
        }
    }
}
