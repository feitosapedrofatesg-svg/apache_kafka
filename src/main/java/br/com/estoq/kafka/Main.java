package br.com.estoq.kafka;

import java.util.Locale;

public class Main {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        try {
            executar(args);
        } catch (Exception erro) {
            System.err.println("Erro: " + erro.getMessage());
            System.exit(1);
        }
    }

    static void executar(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("preparar")) {
            Preparar.criarTopico(Config.TOPICO);
        } else if (args.length == 3 && args[0].equals("produtor")) {
            Produtor.enviar(Config.TOPICO, args[1].toUpperCase(Locale.ROOT), Integer.parseInt(args[2]));
        } else if (args.length > 0 && args[0].equals("consumidor")) {
            int limite = 0;
            int espera = 0;
            boolean somenteLer = false;
            for (int i = 1; i < args.length; i++) {
                switch (args[i]) {
                    case "--somente-ler" -> somenteLer = true;
                    case "--limite" -> limite = Integer.parseInt(args[++i]);
                    case "--espera" -> espera = Integer.parseInt(args[++i]);
                    default -> throw new IllegalArgumentException("Opção desconhecida: " + args[i]);
                }
            }
            if (limite < 0 || espera < 0) {
                throw new IllegalArgumentException("Limite e espera não podem ser negativos.");
            }
            Consumidor.ler(Config.TOPICO, limite, espera, somenteLer, System.out);
        } else {
            throw new IllegalArgumentException("Use: preparar | produtor ENTRADA/SAIDA quantidade | consumidor");
        }
    }
}
