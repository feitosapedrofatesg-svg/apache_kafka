package br.com.estoq.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class Config {
    public static final String TOPICO = System.getenv().getOrDefault("KAFKA_TOPICO", "estoq.movimentacoes");
    public static final ObjectMapper JSON = new ObjectMapper();

    public static Properties propriedades() {
        Properties propriedades = new Properties();
        propriedades.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_SERVIDOR", "kafka:9092"));
        return propriedades;
    }
}
