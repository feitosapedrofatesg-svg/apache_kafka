# Roteiro da dupla: 10 minutos

O cronograma reserva exatamente **5 minutos de teoria e 5 de prática**. Cada pessoa tem 2min30 de teoria e 2min30 na prática, totalizando 5 minutos de participação. As falas são uma base para o ensaio. Usem um cronômetro e ajustem as pausas para trocar para o laboratório em 5:00 e encerrar em 10:00.

## Teoria: 0:00 a 5:00

| Slide | Assunto | Tempo | Quem fala |
|---|---|---|---|
| 1 | Apache Kafka | 0:00 a 0:20 | Pessoa 1 |
| 2 | O que é e para que serve | 0:20 a 1:00 | Pessoa 1 |
| 3 | Origem, criadores e mantenedor | 1:00 a 1:35 | Pessoa 1 |
| 4 | Tecnologia e funcionamento | 1:35 a 2:30 | Pessoa 1 |
| 5 | Partições, offsets e réplicas | 2:30 a 3:25 | Pessoa 2 |
| 6 | Vantagens e desvantagens | 3:25 a 4:05 | Pessoa 2 |
| 7 | Casos de uso e empresas | 4:05 a 4:45 | Pessoa 2 |
| 8 | Laboratório com o estoQ | 4:45 a 5:00 | Pessoa 2 |

### Slide 1: Apache Kafka

**Pessoa 1, 0:00 a 0:20.**

Nosso tema é Apache Kafka, uma plataforma de streaming de eventos. Vamos explicar como ela funciona e depois mostrar uma demonstração inspirada no estoQ, usando entradas e saídas de produtos. Dividimos a apresentação em cinco minutos de teoria e cinco de prática.

### Slide 2: O que é e para que serve

**Pessoa 1, 0:20 a 1:00.**

Um evento é o registro de algo que aconteceu. No estoQ, pode ser a saída de oito unidades de um produto. O Kafka recebe esse registro, guarda o evento e permite que outros programas façam a leitura. Ele ajuda quando precisamos distribuir muitas informações entre sistemas sem que um dependa diretamente da execução do outro. Na origem, o LinkedIn precisava lidar com grandes volumes de logs e atividades. No nosso exemplo, a mesma movimentação poderia alimentar um relatório e um serviço de alertas.

### Slide 3: Origem, criadores e mantenedor

**Pessoa 1, 1:00 a 1:35.**

O Kafka surgiu no LinkedIn em 2010 e teve seu código aberto em 2011. Os principais criadores foram Jay Kreps, Neha Narkhede e Jun Rao. Em 2012, ele se tornou um projeto de alto nível da Apache Software Foundation. Hoje, a fundação e a comunidade mantêm o projeto. Também existem fornecedores comerciais, como a Confluent. É importante separar os papéis: a Apache governa o projeto aberto, enquanto fornecedores oferecem serviços e produtos baseados nessa tecnologia.

### Slide 4: Tecnologia e funcionamento

**Pessoa 1, 1:35 a 2:30.**

O projeto tem código principalmente em Java e Scala e executa na JVM, a máquina virtual Java. Ele pode rodar em servidores ou em contêineres, como faremos com Docker. O produtor é o programa que envia o evento. O tópico organiza eventos de um assunto, como movimentações de estoque. O broker é o servidor que recebe e guarda os eventos, e vários brokers formam um cluster. O consumidor faz a leitura e executa uma ação. Nossos clientes são em Python. No laboratório, o produtor representa o estoQ e o consumidor calcula o saldo. Esse cálculo pertence ao nosso programa, não acontece automaticamente no Kafka.

### Slide 5: Partições, offsets e réplicas

**Pessoa 2, 2:30 a 3:25.**

Um tópico pode ser dividido em partições para distribuir o trabalho. Cada partição tem uma sequência ordenada, e a posição do evento se chama offset. A ordem é garantida dentro de uma partição, não entre todas. Os registros ficam em segmentos de log no disco, com índices por offset e tempo que ajudam a localizar posições. Partições também podem ter réplicas em outros brokers, o que ajuda a recuperar falhas quando a configuração permite. No Kafka atual, o KRaft coordena os metadados. Ler um evento não o apaga: a política de retenção define sua permanência. No laboratório usamos uma partição, um broker e uma réplica, sem tolerância a falhas.

### Slide 6: Vantagens e desvantagens

**Pessoa 2, 3:25 a 4:05.**

Entre as vantagens estão distribuir o processamento por partições e permitir que vários sistemas leiam os mesmos eventos, usando grupos diferentes. Outro ponto é reprocessar o histórico enquanto os dados continuam retidos. Por outro lado, administrar um cluster exige configuração e monitoramento. Guardar eventos e réplicas também usa armazenamento. O consumidor precisa tratar possíveis repetições e falhas. Por isso, para um cadastro pequeno, o banco relacional pode ser suficiente. Nossa avaliação é que Kafka faz mais sentido quando existe um fluxo contínuo de eventos e vários sistemas interessados nesses dados.

### Slide 7: Casos de uso e empresas

**Pessoa 2, 4:05 a 4:45.**

O Kafka pode servir para integrar sistemas, coletar logs e alimentar análises com dados contínuos. Temos exemplos reais: o LinkedIn relata seu uso para eventos e atividade do site. A Uber descreve o Kafka como uma base importante para sistemas de dados em lote e em tempo real. No estoQ, nossa proposta é usar eventos de movimentação para alimentar relatórios e alertas. Isso não muda o papel do banco operacional, que continua responsável pelas transações e regras de estoque. A demonstração usa dados fictícios e é independente do sistema real.

### Slide 8: Laboratório com o estoQ

**Pessoa 2, 4:45 a 5:00.**

Agora vamos para a prática. Vamos registrar uma entrada, duas saídas e uma reposição. O consumidor mostrará o saldo e o alerta de estoque baixo. Também vamos reler o histórico para mostrar que os eventos continuam guardados no Kafka.

## Prática: 5:00 a 10:00

Deixem o ambiente pronto e o tópico vazio antes da apresentação. Os dois terminais devem estar na pasta do projeto. Não contem download, build ou inicialização como parte da demonstração.

| Intervalo | Responsável | Ação |
|---|---|---|
| 5:00 a 5:30 | Pessoa 1 | Mostrar o tópico e abrir o consumidor |
| 5:30 a 6:10 | Pessoa 1 | Publicar entrada de 20 e mostrar o JSON e saldo 20 |
| 6:10 a 6:50 | Pessoa 1 | Publicar saída de 8 e mostrar saldo 12 |
| 6:50 a 7:30 | Pessoa 1 | Publicar saída de 9 e mostrar saldo 3 com alerta |
| 7:30 a 8:20 | Pessoa 2 | Parar o consumidor e iniciá-lo de novo para reler o histórico |
| 8:20 a 9:00 | Pessoa 2 | Explicar offsets e a reconstrução do saldo 3 |
| 9:00 a 9:40 | Pessoa 2 | Publicar entrada de 10 e mostrar saldo 13 |
| 9:40 a 10:00 | Pessoa 2 | Explicar o resultado e encerrar |

### Comandos da Pessoa 1

Mostrar o tópico:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic estoq.movimentacoes
```

No terminal 1:

```bash
docker compose run --rm app python consumidor.py
```

No terminal 2, um de cada vez:

```bash
docker compose run --rm app python produtor.py ENTRADA 20
docker compose run --rm app python produtor.py SAIDA 8
docker compose run --rm app python produtor.py SAIDA 9
```

Fala de apoio: “O produtor representa o estoQ registrando uma movimentação. O Kafka confirma que recebeu o evento. O consumidor lê o JSON e calcula o saldo. Quando o saldo chega a três, nosso programa mostra o alerta porque o mínimo é cinco.”

### Comandos da Pessoa 2

No terminal 1, pressionar Ctrl+C e iniciar de novo:

```bash
docker compose run --rm app python consumidor.py
```

Fala de apoio: “Os eventos continuam guardados mesmo depois da leitura. Esta execução usa um grupo novo e lê desde o início para reconstruir o saldo. Os offsets mostram as posições dos registros dentro da partição.”

No terminal 2:

```bash
docker compose run --rm app python produtor.py ENTRADA 10
```

Fala final: “A reposição acrescenta outro evento e o saldo vai para treze. O Kafka transportou e guardou as movimentações. Nosso consumidor fez o cálculo e o alerta. Esse exemplo mostra como eventos poderiam ajudar o estoQ a alimentar outros serviços.”

## Conferência dos requisitos

| Requisito da teoria | Onde aparece |
|---|---|
| Definição | Slide 2 |
| Tecnologia | Slide 4 |
| Arquitetura, distribuição e indexação | Slides 4 e 5 |
| Origem e ano | Slide 3 |
| Criadores | Slide 3 |
| Mantenedor e fornecedor | Slide 3 |
| Necessidade que resolve | Slide 2 |
| Vantagens e desvantagens | Slide 6 |
| Casos de uso | Slide 7 |
| Empresas que utilizam | Slide 7 |

A prática cobre inserção/publicação, leitura, processamento em tempo real e releitura. UPDATE e DELETE são opcionais para streaming no enunciado. A manipulação dos dados aparece no cálculo do saldo e no alerta pelo consumidor.
