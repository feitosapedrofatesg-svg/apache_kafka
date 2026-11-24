# Resumo para estudar

## 1. Definição

Apache Kafka é uma plataforma distribuída de streaming de eventos. Ela permite publicar, guardar e ler acontecimentos de um sistema continuamente. Um evento registra algo que aconteceu, como uma saída de produto no estoQ. [Introdução oficial](https://kafka.apache.org/41/getting-started/introduction/)

## 2. Tecnologia

O projeto tem código principalmente em **Java e Scala** e executa sobre a **JVM**, a máquina virtual Java. Pode rodar em servidores, máquinas virtuais e contêineres. O laboratório usa a imagem oficial do Kafka no Docker e programas em Python como clientes. A linguagem do cliente não precisa ser a mesma do servidor. [Código do projeto](https://github.com/apache/kafka), [documentação do cliente Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## 3. Arquitetura

- **Producer (produtor):** envia eventos.
- **Topic (tópico):** organiza eventos de um assunto, como `estoq.movimentacoes`.
- **Partition (partição):** divide o tópico em sequências de eventos. A ordem é garantida dentro de cada partição, não entre todas as partições.
- **Broker:** servidor que recebe e armazena eventos. Vários brokers formam um cluster.
- **Consumer (consumidor):** lê os eventos e executa alguma ação.
- **Offset:** posição de um evento dentro de uma partição. O consumidor pode guardar sua posição de leitura.

O Kafka grava os eventos em segmentos de log no disco. Índices por offset e por tempo ajudam a localizar posições, sem funcionar como índices de busca por qualquer campo de um banco relacional. Partições podem ter réplicas em brokers diferentes. Se houver réplicas sincronizadas e configuração adequada, uma delas pode assumir a liderança quando outra falhar. No modo **KRaft**, controladores coordenam os metadados com um protocolo de consenso. O laboratório usa Kafka 4.x com KRaft, sem ZooKeeper. [Conceitos](https://kafka.apache.org/41/getting-started/introduction/), [implementação](https://kafka.apache.org/41/implementation/), [KRaft](https://kafka.apache.org/41/operations/kraft/)

No laboratório há um único broker e uma única partição. Todos os eventos vão para ela. Em uma configuração com mais partições, usar o código do produto como chave ajuda a manter as movimentações daquele produto na mesma partição enquanto o particionamento se mantém estável. A réplica única do laboratório não oferece tolerância à perda do broker.

## 4. Origem

O Kafka surgiu no **LinkedIn em 2010**. A empresa abriu seu código em **2011** para tratar grandes volumes de eventos e logs. Ele entrou na incubadora Apache em 2011 e se tornou um projeto de alto nível da fundação em 2012. [Histórico no LinkedIn](https://www.linkedin.com/blog/engineering/open-source/kafka-ecosystem-at-linkedin), [abertura do código](https://www.linkedin.com/blog/member/archive/open-source-linkedin-kafka), [incubadora Apache](https://incubator.apache.org/projects/kafka.html)

## 5. Criadores

Os principais criadores foram **Jay Kreps, Neha Narkhede e Jun Rao**, que trabalhavam no LinkedIn. Eles assinam o artigo original de 2011. [Artigos no wiki Apache](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/27822226/Kafka+papers+and+presentations)

## 6. Mantenedor

O projeto pertence à **Apache Software Foundation** e tem manutenção da comunidade Apache Kafka. Empresas também oferecem serviços baseados nele. A Confluent é um exemplo de fornecedora comercial, mas não deve ser confundida com a fundação que governa o projeto Apache. [Informações do projeto](https://kafka.apache.org/project/)

## 7. Necessidade que resolve

O LinkedIn precisava distribuir grandes volumes de registros de atividade para diferentes sistemas com baixa latência. O Kafka permite que uma origem publique eventos e vários consumidores os utilizem de forma independente. [Publicação do LinkedIn em 2011](https://www.linkedin.com/blog/engineering/archive/come-linkedin-hear-talk-about-kafka-our-open-source-distributed-pub-sub-messaging-system)

No exemplo do estoQ, uma saída de mercadoria poderia interessar ao relatório e a um serviço de alertas. A aplicação publica o acontecimento e cada consumidor trata sua parte. Isso reduz a dependência direta entre os sistemas. **Essa aplicação ao estoQ é uma proposta didática deste trabalho.**

## 8. Vantagens e desvantagens

| Vantagens | Desvantagens e cuidados |
|---|---|
| Partições permitem distribuir o processamento | O cluster exige configuração e monitoramento |
| Eventos retidos podem ser relidos | Retenção e réplicas ocupam armazenamento |
| Grupos diferentes podem ler o mesmo evento | A ordem é por partição, não global |
| Réplicas podem ajudar na recuperação de falhas | O consumidor precisa lidar com falhas e possíveis duplicidades |

Essa comparação é uma análise a partir da arquitetura. Para um cadastro pequeno com poucas operações, um banco relacional e chamadas diretas podem ser suficientes. O Kafka passa a fazer mais sentido quando há vários consumidores, volume de eventos ou necessidade de reprocessamento. [Arquitetura e garantias de entrega](https://kafka.apache.org/41/design/design/)

## 9. Casos de uso

Exemplos: integração entre sistemas, coleta de logs, acompanhamento de transações, sensores e alimentação contínua de plataformas analíticas. No estoQ, poderíamos usar eventos de movimentação para alimentar relatórios e alertas. O banco operacional continuaria cuidando das transações e das regras de estoque. [Casos de uso oficiais](https://kafka.apache.org/41/getting-started/uses/)

## 10. Empresas que utilizam

- **LinkedIn:** usa Kafka na infraestrutura de eventos e dados, incluindo registros de atividade do site. [Relato da engenharia](https://www.linkedin.com/blog/engineering/open-source/kafka-ecosystem-at-linkedin)
- **Uber:** relata o uso de Kafka como base de sistemas de dados em lote e em tempo real. [Relato da engenharia, 2024](https://www.uber.com/us/en/blog/kafka-tiered-storage/)

São exemplos públicos documentados pelas próprias empresas. Não usamos números de volume como se fossem métricas atuais.

## Perguntas que podem aparecer

**Kafka é um banco de dados?**
Ele armazena eventos, mas sua função principal neste trabalho é o streaming. O acesso é organizado por tópicos, partições e offsets. Consultas relacionais e cadastros continuam no banco operacional.

**Ler o evento apaga a mensagem?**
Não. A leitura não apaga o evento. O Kafka mantém os dados de acordo com a política do tópico. Nosso tópico usa retenção de 7 dias e política `delete`.

**Por que não mostramos UPDATE e DELETE?**
O exercício permite dispensá-los em streaming. Aqui acrescentamos novos eventos ao log. Uma correção de negócio deve gerar outro evento. A exclusão por retenção e a compactação são mecanismos diferentes de editar uma linha individual.

**Quem calcula o estoque?**
O consumidor Python, em `estoque.py`. O Kafka entrega e guarda os eventos.

**Por que o consumidor lê tudo de novo quando reinicia?**
Ele começa com o saldo em memória vazio e cria um grupo novo. Assim, lê desde o início retido para reconstruir o saldo. Um consumidor real pode usar um grupo fixo e checkpoints, combinados com estado persistente.

**Kafka garante exatamente uma vez em qualquer situação?**
Não automaticamente. Existem recursos de idempotência e transações para cenários específicos. Este exemplo confirma o envio e evita repetir IDs em memória, mas não implementa uma transação entre Kafka e um banco externo.

**Como seria a integração real com o estoQ?**
Seria preciso publicar as movimentações confirmadas, por exemplo com uma tabela de outbox gravada na mesma transação do estoque e um processo que envia esses registros ao Kafka. Isso evitaria perder o evento entre salvar a movimentação e publicá-la. Essa integração não faz parte do laboratório.

**O que acontece se a saída for maior que o saldo?**
O consumidor projeta os eventos recebidos e pode mostrar um valor negativo. Na integração real, o estoQ validaria a saída antes de confirmar a movimentação. O consumidor não substitui essa regra transacional.

**E Data Lake, modelo estrela ou arquitetura medalhão?**
A exigência especial é dos temas 19 a 23. Nosso tema é o 18, Kafka. Ele pode alimentar esses ambientes, mas não precisamos implementar um Data Warehouse para demonstrá-lo.
