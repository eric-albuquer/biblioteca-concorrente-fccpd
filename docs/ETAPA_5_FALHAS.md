# Etapa 5 — Consistência, falhas e recuperação

## Status

Concluída em 21/11/2026. Os cenários usam PostgreSQL e RabbitMQ reais; mocks não
são usados para comprovar commit, redelivery ou ACK.

## Cobertura do plano

| Requisito | Evidência automatizada |
|---|---|
| 2+ workers disputando um exemplar | `test_dois_workers_executam_ciclo_completo_e_transferencia_fifo` |
| FIFO com 3+ solicitantes e devoluções | `test_disputa_concorrente_preserva_estoque_e_fifo` usa 8 solicitantes |
| `AGUARDANDO` após reinício da camada de serviço | `test_aguardando_e_request_id_sobrevivem_a_novas_instancias` |
| Mesmo UUID antes/depois do processamento | `test_request_id_repetido_antes_e_depois_do_processamento` |
| Crash antes do commit | `test_queda_real_antes_do_commit_faz_rollback_e_redelivery` |
| Crash depois do commit e antes do ACK | `test_queda_real_depois_do_commit_redelivery_nao_duplica` |
| Workers offline e retorno | `test_workers_offline_processam_todos_os_pedidos_ao_retornar`; cinco comandos entram pelo Gateway TCP |
| RabbitMQ offline e recuperação da outbox | `test_publisher_preserva_outbox_quando_broker_esta_indisponivel` |
| Mensagem inválida/retry esgotado | testes de DLQ em `test_messaging.py` |
| Script determinístico e invariantes | `scripts/demo_falha.py` e `scripts/validar_invariantes.py` |

O teste de workers offline confirma três pedidos `CONCLUIDO` e dois
`AGUARDANDO` para um título com três exemplares. A suíte existente também cobre
mensagens fora de ordem, redelivery duplicado, devolução inválida e conflito de
payload para o mesmo UUID.

## Pontos de falha controlados

Os failpoints ficam desabilitados por padrão e só são acionados quando
`ENABLE_FAILPOINTS=1`. `FAILPOINT_REQUEST_ID` restringe a falha a um comando e
um arquivo marcador torna a falha *one-shot* durante a demonstração.

- `BEFORE_COMMIT` é chamado dentro da transação, imediatamente antes de sair do
  contexto transacional. O encerramento com código 91 desfaz as alterações e a
  conexão RabbitMQ devolve a entrega à fila.
- `AFTER_COMMIT_BEFORE_ACK` é chamado depois que o serviço retorna — portanto,
  depois do commit — e antes de `basic_ack`. O código 92 encerra o processo; o
  novo worker recebe a mensagem e encontra a operação já concluída.

Foi mantida a regra operacional `commit -> ACK`. Em produção, os failpoints não
devem ser habilitados.

## Demonstração reproduzível

Use um ambiente de teste. A demo exige fila, DLQ e outbox vazias e recusa
executar se houver outro consumidor conectado. Ela cria dois livros de teste e
mantém o estado final no banco para inspeção.

Com a infraestrutura do Compose:

```powershell
docker compose up -d postgres rabbitmq
docker compose run --rm migrate
docker compose stop worker publisher
docker compose run --rm --no-deps gateway python -m scripts.demo_falha
```

O script sobe um Gateway TCP real numa porta efêmera, envia os comandos com
`Cliente`, publica a outbox, provoca cada crash em um subprocesso do worker,
inicia outro subprocesso para a recuperação e imprime um relatório JSON com
UUIDs, logs e estados inicial/intermediário/final.

O validador pode ser repetido isoladamente:

```powershell
docker compose run --rm --no-deps gateway python -m scripts.validar_invariantes
```

Ele termina com código diferente de zero se detectar estoque inválido,
divergência entre capacidade e empréstimos ativos, empréstimo duplicado,
violação FIFO, conclusão sem empréstimo, devolução incompleta ou mensagem na
DLQ. O JSON também lista fila persistente, pedidos atendidos, outbox pendente e
contagem da DLQ.

## Evidência real observada

Execução de 21/11/2026 contra PostgreSQL 16.16 e RabbitMQ 4.4.1 em containers:

### Antes do commit

- `request_id`: `7f20f376-f093-46b4-9bb3-811f89c65d23`;
- estado inicial: capacidade 1, disponíveis 1, ativos 0, fila RabbitMQ 0;
- log do crash: `FAILPOINT=BEFORE_COMMIT exit_code=91`;
- após o crash: solicitação `RECEBIDO`, disponíveis 1, ativos 0 e uma mensagem
  pronta para redelivery;
- após recuperação: solicitação `CONCLUIDO`, disponíveis 0, ativos 1, fila 0 e
  DLQ 0;
- log de recuperação: `request_id=7f20f376-f093-46b4-9bb3-811f89c65d23 tipo=PEDIR processado`.

### Depois do commit e antes do ACK

- `request_id`: `997f4266-74ca-4556-8821-ad08903ab4e6`;
- estado inicial: capacidade 1, disponíveis 1, ativos 0, fila RabbitMQ 0;
- log do crash: `FAILPOINT=AFTER_COMMIT_BEFORE_ACK exit_code=92`;
- após o crash: solicitação já `CONCLUIDO`, disponíveis 0, ativos 1 e uma
  mensagem pronta para redelivery;
- após recuperação: o mesmo `emprestimo_id=4`, disponíveis 0, exatamente um
  empréstimo ativo, fila 0 e DLQ 0;
- log de recuperação: `request_id=997f4266-74ca-4556-8821-ad08903ab4e6 tipo=PEDIR processado`.

O relatório final do validador retornou `ok=true`, sem violações FIFO, sem
`borrow_request_id` duplicado, outbox pendente 0 e DLQ 0.

## Testes executados

Ambiente focado da etapa:

```text
test/test_falhas.py: 6 passed in 5.06s
scripts.demo_falha: exit code 0, resultado APROVADO
suíte completa: 42 passed in 12.96s
```

A regressão completa foi executada com `TEST_DATABASE_URL` e
`TEST_RABBITMQ_URL` apontando para instâncias reais. Para repeti-la:

```powershell
python -m pytest -v
```

## Limites

- PostgreSQL e RabbitMQ continuam sendo instâncias únicas; a etapa comprova
  recuperação do processo consumidor, não alta disponibilidade da infraestrutura.
- O teste de permanência de `AGUARDANDO` recria os objetos de serviço e as
  conexões. A preservação por reinício completo do PostgreSQL/volume já foi
  validada na Etapa 4.
- A entrega RabbitMQ permanece *at-least-once*. A ausência de duplicação vem da
  transação e das chaves idempotentes no PostgreSQL, não de entrega exatamente
  uma vez pelo broker.

## Próxima etapa

A Etapa 6 medirá throughput com 1, 2 e 4 workers e comparará relatório
CPU-bound sequencial e paralelo, sem usar estes failpoints nas medições.
