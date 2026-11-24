# Entendendo o código Java

O laboratório usa **Java 21, Maven e o cliente oficial do Kafka**. As classes ficam no pacote `br.com.estoq.kafka`, em `src/main/java`. Não precisa de Spring para este exemplo.

## Por onde começar

1. Abra o `pom.xml` na sua IDE como projeto Maven.
2. Leia `Evento.java`, que representa os dados da movimentação.
3. Leia `Estoque.java`, que soma entradas e desconta saídas.
4. Leia `Produtor.java` e `Consumidor.java` para entender a comunicação com o Kafka.

| Classe | Responsabilidade |
|---|---|
| `Main` | Lê o comando digitado e chama a classe correspondente |
| `Config` | Guarda a configuração da conexão e o conversor JSON |
| `Evento` | Guarda ID, produto, tipo, quantidade e data |
| `Estoque` | Calcula o saldo em memória e identifica IDs já processados |
| `Preparar` | Cria o tópico usando a API administrativa |
| `Produtor` | Publica uma movimentação com `KafkaProducer` |
| `Consumidor` | Lê as mensagens com `KafkaConsumer` e mostra o saldo |

## Conceitos de Java que aparecem

- **`record`:** uma forma curta de declarar uma classe para guardar dados. Por exemplo, `evento.quantidade()` retorna a quantidade.
- **`HashMap`:** guarda o saldo de cada produto usando o código do produto como chave.
- **`HashSet`:** guarda os IDs já processados para não somar o mesmo evento duas vezes na mesma execução.
- **`try` com recursos:** fecha as conexões do produtor, consumidor e administrador.
- **Jackson:** transforma o objeto `Evento` em JSON e o JSON novamente em objeto.

## Como o produtor funciona

Em `Produtor.java`, o programa cria um `Evento`, converte para JSON e monta um `ProducerRecord`. A chave é o código do produto e o valor é o JSON.

```java
ProducerRecord<String, String> registro =
        new ProducerRecord<>(topico, evento.produto(), json);
```

O método `produtor.send(registro)` envia a mensagem. O programa espera a confirmação e mostra o tópico, a partição e o offset retornados pelo Kafka.

## Como o consumidor funciona

Em `Consumidor.java`, `subscribe` escolhe o tópico e `poll` busca mensagens. Para cada registro, o programa transforma o JSON em `Evento` e chama o cálculo:

```java
Estoque.Resultado resultado = estoque.aplicar(evento);
```

O programa mostra o saldo. Se estiver abaixo de 5, mostra o alerta. Essa regra está no nosso código Java.

## Compilar e testar

Com JDK 21 e Maven no computador, na raiz do projeto:

```bash
mvn test
mvn package
```

O primeiro comando testa o cálculo sem precisar do Kafka. O segundo também gera `target/estoq-kafka.jar`, com as dependências. A classe de entrada do JAR é `Main`.

Para executar o laboratório, siga [LABORATORIO.md](LABORATORIO.md). O Docker configura a rede e já chama `java -jar /lab/estoq-kafka.jar`. Por exemplo:

```bash
docker compose run --rm app produtor ENTRADA 20
```

Nesse comando, `produtor ENTRADA 20` chega ao `String[] args` do método `main`. O Kafka fica acessível pelo nome `kafka` dentro da rede Docker. Por isso, os comandos completos da demonstração usam o contêiner da aplicação.

Para testar a integração com Kafka real:

```bash
docker compose build testes
docker compose run --rm testes
```

O serviço `testes` executa `mvn verify -Pkafka-real`. O Maven roda os testes de cálculo e o teste `LaboratorioIT`, que usa um tópico temporário.
