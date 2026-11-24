# Laboratório: movimentações do estoQ

## Antes da aula

1. Instale e inicie o Docker com Compose v2.
2. Baixe o repositório e entre na pasta `apache_kafka`.
3. Prepare os serviços usando os comandos abaixo.
4. Ensaie a sequência inteira e depois limpe somente o volume do laboratório.
5. Prepare novamente e deixe o Kafka iniciado, o tópico vazio e dois terminais abertos.

```bash
docker compose build app
docker compose up -d --wait kafka
docker compose run --rm app python preparar.py
docker compose ps
```

O Kafka deve aparecer como `healthy`. O tempo de download e de inicialização fica fora dos 5 minutos de demonstração.

## 1. Conferir o tópico

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic estoq.movimentacoes
```

Mostre o nome do tópico, `PartitionCount: 1` e `ReplicationFactor: 1`.

Fala sugerida: “Este tópico guarda as movimentações do estoque. Para a aula, usamos uma partição e um broker.”

## 2. Abrir o consumidor no terminal 1

```bash
docker compose run --rm app python consumidor.py
```

Ele fica esperando eventos. A mensagem inicial aparece antes da atribuição da partição. Os eventos enviados nesse intervalo também serão lidos, pois ficam guardados no Kafka.

## 3. Enviar eventos no terminal 2

Execute um comando de cada vez e mostre a mudança no terminal 1.

```bash
docker compose run --rm app python produtor.py ENTRADA 20
```

Saldo: **20 unidades**.

```bash
docker compose run --rm app python produtor.py SAIDA 8
```

Saldo: **12 unidades**.

```bash
docker compose run --rm app python produtor.py SAIDA 9
```

Saldo: **3 unidades**, com alerta de estoque abaixo do mínimo de 5.

O JSON contém ID, produto, tipo, quantidade e data. A chave Kafka é `ARROZ-5KG`. Em um tópico novo, os offsets dessas mensagens são 0, 1 e 2, todos na partição 0.

## 4. Reler e reconstruir o saldo

No terminal 1, pressione **Ctrl+C** e rode novamente:

```bash
docker compose run --rm app python consumidor.py
```

O programa relê os três eventos e reconstrói os saldos 20, 12 e 3. A leitura anterior não apagou os eventos. O grupo novo e a opção `earliest` permitem essa releitura do histórico ainda retido.

## 5. Registrar uma reposição

No terminal 2:

```bash
docker compose run --rm app python produtor.py ENTRADA 10
```

Saldo: **13 unidades**. O consumidor não emite um novo alerta para esse saldo. Os eventos anteriores continuam no histórico.

Fala sugerida: “A reposição vira um novo evento. O Kafka guarda a movimentação e nosso consumidor recalcula o saldo.”

## Leitura somente do JSON, para estudar

Este comando é opcional e não precisa entrar nos 5 minutos:

```bash
docker compose run --rm app python consumidor.py --somente-ler --limite 4 --espera 20
```

Ele lê os quatro eventos sem calcular saldo e encerra. Use após a sequência completa e com o tópico recém-preparado para o ensaio.

## Reiniciar o ensaio do zero

Pare todos os consumidores com Ctrl+C. Depois execute:

```bash
docker compose down -v
docker compose up -d --wait kafka
docker compose run --rm app python preparar.py
```

**`down -v` apaga o histórico do laboratório.** O projeto Compose tem o nome `estoq-kafka-lab`, separado do estoQ. Faça isso antes de começar outro ensaio, nunca no meio da apresentação. Se não limpar, os eventos de ensaios anteriores também entram no cálculo.

## Se algo não funcionar

| Situação | O que conferir |
|---|---|
| Docker não responde | Inicie o Docker Desktop ou o serviço Docker e confira a permissão de acesso |
| Kafka demora para ficar pronto | Use `docker compose logs --tail 60 kafka` e aguarde o healthcheck |
| Tópico não existe | Execute `docker compose run --rm app python preparar.py` |
| Saldo diferente do roteiro | Veja se ainda existem eventos de um ensaio anterior |
| Sem internet na aula | Faça o build e baixe as imagens antes da apresentação |
| Consumidor encerrou | Leia o erro, confira `docker compose ps` e inicie o consumidor novamente |

Depois de alterar arquivos Python, execute `docker compose build app` outra vez, porque o código é copiado para a imagem.
