package br.com.estoq.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class Preparar {
    public static void criarTopico(String nome) throws Exception {
        try (Admin admin = Admin.create(Config.propriedades())) {
            NewTopic topico = new NewTopic(nome, 1, (short) 1);
            topico.configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"));
            try {
                admin.createTopics(List.of(topico)).all().get(30, TimeUnit.SECONDS);
                System.out.println("Tópico criado: " + nome);
            } catch (ExecutionException erro) {
                if (!(erro.getCause() instanceof TopicExistsException)) {
                    throw erro;
                }
                System.out.println("Tópico já existe: " + nome);
            }
            System.out.println("Configuração do laboratório: 1 partição, 1 réplica e retenção de 7 dias.");
        }
    }
}
