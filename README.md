# Apache Kafka com o exemplo do estoQ

Trabalho do tema **18: plataforma de streaming de eventos**.

A ideia é mostrar o Kafka usando uma situação fácil de entender: entradas e saídas de produtos no estoQ. Um programa envia as movimentações e outro acompanha os eventos para calcular o saldo e avisar quando o estoque fica baixo.

Este é um **laboratório independente, inspirado no estoQ**, com dados fictícios. O programa `Produtor.java` representa o sistema enviando eventos. Não existe conexão com a aplicação ou com o banco real do estoQ neste repositório.

## Material da apresentação

- [Slides em PowerPoint](apresentacao/apache-kafka-estoq.pptx)
- [Roteiro de fala e cronograma de 10 minutos](docs/ROTEIRO.md)
- [Resumo dos 10 tópicos e respostas para perguntas](docs/TEORIA.md)
- [Passo a passo do laboratório](docs/LABORATORIO.md)
- [Fontes da pesquisa](docs/FONTES.md)

Os slides têm 5 minutos de teoria. Os outros 5 minutos são para executar os comandos do laboratório. As notas do PowerPoint também incluem as falas e as fontes. No roteiro, **Pessoa 1** e **Pessoa 2** representam os integrantes da dupla.

## O que acontece na prática

```text
Produtor (representando o estoQ)
    publica uma movimentação em JSON
Kafka: tópico estoq.movimentacoes
    guarda o evento e permite sua leitura
Consumidor Java
    calcula saldo e exibe alerta de estoque baixo
```

| Movimentação | Quantidade | Saldo esperado |
|---|---:|---:|
| Entrada de arroz | 20 | 20 |
| Saída de arroz | 8 | 12 |
| Saída de arroz | 9 | 3, com alerta |
| Nova entrada de arroz | 10 | 13 |

O Kafka transporta e armazena os eventos. **O código Java faz o cálculo e o alerta.** A nova entrada acrescenta outro evento ao histórico. Ela não edita eventos anteriores.

## Como preparar

É preciso ter **Docker com Docker Compose v2** funcionando. No Windows ou macOS, pode usar Docker Desktop. No Linux, o usuário precisa ter acesso ao Docker. A instalação precisa de internet na primeira preparação para baixar imagens e dependências. Reserve cerca de 2 GB de RAM livres para o laboratório.

```bash
git clone https://github.com/feitosapedrofatesg-svg/apache_kafka.git
cd apache_kafka
docker compose build app
docker compose up -d --wait kafka
docker compose run --rm app preparar
```

Faça isso **antes da aula**. Os comandos da demonstração usam as imagens já preparadas. O Docker compila e executa o Java, sem exigir JDK ou Maven no computador. O código usa **Java 21 e Maven**, com o cliente oficial `kafka-clients`. O laboratório usa Kafka **4.1.2**, uma versão fixada para reproduzir o ambiente, em modo KRaft.

## Demonstração rápida

Abra dois terminais na pasta do projeto.

**Terminal 1: acompanhar os eventos e o saldo**

```bash
docker compose run --rm app consumidor
```

**Terminal 2: enviar uma movimentação por vez**

```bash
docker compose run --rm app produtor ENTRADA 20
docker compose run --rm app produtor SAIDA 8
docker compose run --rm app produtor SAIDA 9
```

O terminal 1 mostra os saldos **20, 12 e 3**. No saldo 3, aparece o alerta de estoque abaixo do mínimo de 5 unidades.

Pare o consumidor com **Ctrl+C** e execute o mesmo comando de novo. Ele relê os eventos e reconstrói o saldo 3. Depois envie:

```bash
docker compose run --rm app produtor ENTRADA 10
```

O saldo passa para **13**. Veja o [roteiro do laboratório](docs/LABORATORIO.md) para consultar o tópico, ler apenas o JSON e preparar outro ensaio.

## Código Java para estudar

Abra o `pom.xml` como projeto Maven no IntelliJ, Eclipse ou VS Code. O código fica em `src/main/java/br/com/estoq/kafka`. Veja [a explicação das classes Java](docs/JAVA.md). Para compilar e testar o cálculo fora do Docker, instale JDK 21 e Maven e execute `mvn test`.

O Docker inicia `java -jar /lab/estoq-kafka.jar`. Por isso, basta escrever `app produtor`, `app consumidor` ou `app preparar` nos comandos. Não usamos Spring neste exemplo.

## Organização dos arquivos

| Arquivo | Função |
|---|---|
| `pom.xml` | Define as dependências e o build Maven |
| `docker-compose.yml` | Sobe o Kafka e configura os programas do laboratório |
| `Preparar.java` | Cria o tópico com uma partição |
| `Produtor.java` | Envia entrada ou saída em JSON |
| `Consumidor.java` | Lê os eventos e mostra saldo e alerta |
| `Estoque.java` | Contém o cálculo de saldo |
| `src/test/java/.../EstoqueTest.java` | Testa cálculo, repetição e dados inválidos |
| `src/test/java/.../LaboratorioIT.java` | Testa envio, leitura e reprocessamento com Kafka real |

## Testes

Com o Kafka iniciado, rode os testes JUnit e a integração pelo Maven no Docker:

```bash
docker compose build testes
docker compose run --rm testes
```

Para testar apenas o cálculo, use `mvn test` com JDK 21 e Maven locais.

O teste de integração cria e apaga somente um tópico temporário de teste. Ele não usa o histórico da apresentação.

## Limites deste exemplo

Usamos um broker, uma partição e uma réplica para facilitar a aula. Isso não demonstra tolerância a falhas. Os serviços se comunicam dentro da rede do Docker e não publicam portas no computador.

O consumidor cria um grupo novo em cada execução e lê desde o primeiro evento ainda retido. Seu saldo fica em memória e pode ser reconstruído enquanto o histórico necessário existir. A retenção do tópico é de 7 dias. Para o exemplo, isso basta. Um sistema real precisaria de estado persistente ou snapshots e de uma estratégia de recuperação compatível com sua retenção.

O consumidor evita aplicar novamente o mesmo ID na mesma execução. Essa proteção em memória não oferece garantia de processamento exatamente uma vez entre sistemas. A validação de saídas e do saldo disponível continuaria na transação do estoQ. Aqui calculamos uma projeção dos eventos recebidos.

## Encerrar

```bash
docker compose down
```

Esse comando mantém os eventos no volume. Para apagar **somente os dados deste laboratório** e ensaiar do zero, veja a etapa de reinício em [LABORATORIO.md](docs/LABORATORIO.md).
