# Etapa 3 — RabbitMQ, publisher e workers

Esta etapa completa o caminho assíncrono real:

```text
Cliente TCP -> Gateway -> PostgreSQL/outbox -> Publisher -> RabbitMQ
            -> Worker(s) -> transação de domínio no PostgreSQL -> ack
```

O PostgreSQL continua sendo a fonte canônica. A mensagem contém correlação e
serve como gatilho; o worker carrega a solicitação persistida antes de aplicar
qualquer regra.

## Topologia RabbitMQ

| Recurso | Nome | Configuração |
|---|---|---|
| Exchange de comandos | `biblioteca` | `direct`, durável |
| Fila principal | `biblioteca.comandos` | durável |
| Routing key | `biblioteca.comandos` | binding da fila principal |
| Dead-letter exchange | `biblioteca.dlx` | `direct`, durável |
| Dead-letter queue | `biblioteca.comandos.dlq` | durável |
| Routing key da DLQ | `biblioteca.comandos.dlq` | configurada na fila principal |

As declarações são idempotentes e compartilhadas por publisher, worker e
testes. Mensagens são publicadas com `delivery_mode=2`, `message_id=request_id`
e `content_type=application/json`.

## Publisher da outbox

`OutboxPublisher` lê eventos ainda não publicados em ordem de criação. Antes de
enviar, declara a topologia e ativa publisher confirms. A linha só recebe
`publicado_em` depois que `basic_publish(..., mandatory=True)` foi confirmado.

Se o broker estiver indisponível:

- `publicado_em` continua nulo;
- `tentativas` é incrementado;
- `erro_publicacao` registra uma descrição diagnosticável;
- o laço fecha a conexão e tenta novamente com backoff exponencial limitado.

Se ocorrer falha depois da publicação confirmada e antes da marcação no banco,
o evento pode ser publicado outra vez. Isso é esperado na entrega ao menos uma
vez; os workers são idempotentes. O escopo atual usa um publisher. Escalar
publishers exigirá lease ou reivindicação persistente de lote.

## Worker, commit e acknowledge

Cada `Worker` possui sua própria conexão RabbitMQ e cria conexões PostgreSQL por
transação. A configuração inicial usa `basic_qos(prefetch_count=1)` e
`auto_ack=False`.

Fluxo de consumo:

1. validar tamanho, UTF-8, JSON, UUID, tipo e `message_id`;
2. carregar a solicitação canônica pelo `request_id`;
3. bloquear a linha do livro e aplicar estoque, empréstimo, fila e status;
4. confirmar a transação PostgreSQL;
5. somente então executar `basic_ack`.

Uma mensagem PEDIR fora de ordem não escolhe o destinatário: ela faz o serviço
examinar todas as solicitações `RECEBIDO`/`AGUARDANDO` por `ordem_fila`. Uma
mensagem duplicada encontra o estado já aplicado e recebe ack sem criar outro
empréstimo.

## Retry e DLQ

- mensagem malformada ou inconsistente: `basic_nack(requeue=False)` e DLQ;
- erro de negócio: serviço persiste `REJEITADO`, confirma a transação e o worker
  envia ack;
- erro transitório: republicação persistente e confirmada na fila principal
  com header `x-retry-count`, seguida de ack da entrega anterior;
- retry esgotado: `basic_nack(requeue=False)` e DLQ;
- falha ao republicar o retry: `nack(requeue=True)` para não perder a entrega
  enquanto o próprio broker está instável.

O padrão é três retries além da tentativa inicial. O número é configurável por
worker. Essa estratégia é entrega ao menos uma vez e não deve ser descrita como
execução exatamente uma vez.

As decisões seguem a documentação oficial de
[acknowledgements e publisher confirms](https://www.rabbitmq.com/docs/confirms)
e de [dead-letter exchanges](https://www.rabbitmq.com/docs/dlx).

## Execução em processos separados

Configure ambos os serviços:

```powershell
$env:DATABASE_URL = 'postgresql://usuario:senha@localhost:5432/biblioteca'
$env:RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/%2F'
```

Inicie em terminais separados:

```powershell
# terminal 1
.\.venv\Scripts\python.exe -m network.servidor

# terminal 2
.\.venv\Scripts\python.exe -m messaging.publisher

# terminal 3
$env:WORKER_ID='worker-1'
.\.venv\Scripts\python.exe -m workers.emprestimos

# terminal 4
$env:WORKER_ID='worker-2'
.\.venv\Scripts\python.exe -m workers.emprestimos
```

O empacotamento e a inicialização conjunta desses processos serão automatizados
por Docker Compose na Etapa 4.

## Testes automatizados

```powershell
$env:TEST_DATABASE_URL = `
  'postgresql://biblioteca:biblioteca_test@127.0.0.1:5544/biblioteca_test'
$env:TEST_RABBITMQ_URL = `
  'amqp://guest:guest@127.0.0.1:5673/%2F'
.\.venv\Scripts\python.exe -m pytest -v
```

Os testes específicos desta etapa verificam:

- confirmação antes de marcar a outbox;
- indisponibilidade do broker e publicação posterior;
- dois workers ativos e compartilhando o mesmo estado;
- caminho TCP completo PEDIR → RECEBIDO → CONCLUIDO/AGUARDANDO;
- DEVOLVER e transferência direta ao primeiro da fila;
- mensagem malformada encaminhada à DLQ;
- retry limitado e DLQ após esgotamento;
- mensagens duplicadas e entregues fora de ordem sem dupla alocação.

## Evidências observadas em 2026-11-21

- Python 3.13.4, psycopg 3.3.9 e pika 1.4.4;
- PostgreSQL 16.16 e RabbitMQ 4.4.1 em containers temporários;
- 36 testes aprovados em 8,93 s;
- dois workers receberam pelo menos uma mensagem cada no ciclo completo;
- com capacidade 1, usuário 1 recebeu o empréstimo, usuário 2 ficou
  `AGUARDANDO`, e a devolução transferiu a unidade ao usuário 2 mantendo
  `disponiveis=0`;
- mensagem duplicada e ordem B/A/B resultaram em um único empréstimo para A;
- com `max_retries=2`, houve três tentativas totais e uma mensagem na DLQ;
- broker em porta indisponível manteve o evento na outbox e a publicação foi
  concluída quando uma conexão válida foi usada.

Esses tempos são evidências funcionais, não resultados de benchmark.

## Limitações atuais

- PostgreSQL e RabbitMQ usados nesta etapa são instâncias únicas, portanto
  continuam sendo pontos únicos de falha;
- a política de retry não usa atraso por mensagem; o backoff de reconexão cobre
  indisponibilidade do broker, e uma retry queue com TTL só deve ser adicionada
  se medições mostrarem necessidade;
- o publisher único atende ao escopo atual;
- failpoints antes/depois do commit serão adicionados e demonstrados na Etapa 5;
- containers persistentes, healthchecks e escala declarativa pertencem à
  Etapa 4.

