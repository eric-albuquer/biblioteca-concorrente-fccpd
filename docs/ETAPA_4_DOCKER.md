# Etapa 4 — Docker e execução ponta a ponta

A Parte 2 agora pode ser iniciada como serviços separados com um único comando:

```powershell
docker compose up -d --build --scale worker=2
```

## Serviços

| Serviço | Processo | Dependências de inicialização |
|---|---|---|
| `postgres` | PostgreSQL 16.16 | healthcheck `pg_isready` |
| `rabbitmq` | RabbitMQ 4.4.1 + management | healthcheck `rabbitmq-diagnostics ping` |
| `migrate` | schema e seed idempotentes | PostgreSQL saudável; termina em código 0 |
| `gateway` | `python -m network.servidor` | migração concluída |
| `publisher` | `python -m messaging.publisher` | migração concluída e RabbitMQ saudável |
| `worker` | `python -m workers.emprestimos` | migração concluída e RabbitMQ saudável |

O serviço `worker` não define `container_name`, portanto pode ser escalado. Cada
réplica recebe hostname próprio e mantém conexões RabbitMQ/PostgreSQL
independentes.

## Imagem da aplicação

O `Dockerfile` usa Python `3.13.4-slim-bookworm`, instala somente dependências de
runtime fixadas e executa como usuário de sistema `biblioteca`, não como root.
`tini`, habilitado por `init: true`, trata o processo Python como filho e ajuda
no encaminhamento de sinais.

SIGINT/SIGTERM acionam desligamento controlado no Gateway, publisher e worker.
Transações abertas sofrem rollback e entregas não confirmadas pelo worker podem
ser reenviadas pelo RabbitMQ.

## Rede e configuração

Dentro da rede Compose:

- PostgreSQL: `postgres:5432`;
- RabbitMQ: `rabbitmq:5672`;
- Gateway: `gateway:9999`.

Nenhum container usa `localhost` para outro serviço. As portas de `.env` são
publicações no host e podem ser alteradas sem mudar as URLs internas.

O arquivo `.env.example` contém nomes de variáveis e valores apenas para
desenvolvimento. Copie-o para `.env`, troque as senhas e não versione `.env`.
Senhas usadas em URLs precisam ser URL-safe ou percent-encoded.

## Volumes e reinício

- `postgres_data`: catálogo, solicitações, empréstimos e outbox;
- `rabbitmq_data`: filas, mensagens persistentes e metadados do broker.

As políticas `restart: unless-stopped` recuperam os serviços de longa duração.
`migrate` usa `restart: "no"` porque é uma tarefa finita e idempotente.

```powershell
# preserva volumes
docker compose down

# apaga definitivamente banco e filas
docker compose down -v
```

## Inicialização do zero

```powershell
Copy-Item .env.example .env
# Edite .env e troque as senhas.

docker compose up -d --build --scale worker=2
docker compose ps
```

O estado esperado é:

- `postgres`, `rabbitmq`, `gateway`, `publisher` e duas réplicas de `worker`
  em execução e saudáveis;
- `migrate` em `Exited (0)`.

O Compose atual também aceita espera explícita:

```powershell
docker compose up -d --wait --scale worker=2
```

## Demonstração a partir do host

O cliente usa apenas a biblioteca padrão e pode executar fora dos containers:

```powershell
py -3.13 -m scripts.demo_fluxo --host 127.0.0.1 --port 9999
```

O script:

1. envia `PEDIR` e imprime `RECEBIDO`;
2. consulta até `CONCLUIDO`;
3. envia `DEVOLVER` pelo `emprestimo_id`;
4. consulta até a devolução ficar `CONCLUIDO`.

## Comandos operacionais

```powershell
docker compose ps
docker compose logs --tail=100 gateway publisher worker rabbitmq postgres
docker compose restart gateway publisher worker
```

RabbitMQ Management fica em `http://127.0.0.1:15672` por padrão.

## Evidências observadas em 2026-11-21

A validação foi feita em projeto isolado `biblioteca-etapa4-test`, inicialmente
sem containers ou volumes:

```powershell
docker compose -p biblioteca-etapa4-test up -d --build --scale worker=2
```

Resultados:

- imagem construída com sucesso a partir de `python:3.13.4-slim-bookworm`;
- sete containers criados: PostgreSQL, RabbitMQ, migração, Gateway, publisher e
  dois workers;
- todos os serviços longos ficaram saudáveis e `migrate` terminou em código 0;
- imagem executou como `uid=999(biblioteca)`, sem root;
- ambiente interno reportou `database_host=postgres` e
  `rabbitmq_host=rabbitmq`;
- cliente no host acessou `127.0.0.1:9999` e concluiu PEDIR/DEVOLVER;
- worker `213a5024a968` processou PEDIR e worker `213d2a44aa48` processou
  DEVOLVER, comprovando processos separados;
- UUID do pedido `ca87a2ba-bcfa-4b2d-9947-f4fd1c200f7d` e UUID da devolução
  `26fb8e22-92c0-4d0f-b737-76c9647c9691` permaneceram `CONCLUIDO` após restart
  de PostgreSQL, RabbitMQ, Gateway, publisher e workers;
- todos os containers e a rede foram removidos com `docker compose down`, a
  pilha foi recriada usando somente os volumes preservados e os mesmos UUIDs
  continuaram `CONCLUIDO`;
- a migração rodou novamente sobre os mesmos volumes sem duplicar catálogo ou
  falhar.
- a suíte completa foi repetida contra PostgreSQL/RabbitMQ fornecidos pelo
  Compose: 36 testes aprovados em 10,74 s.

Foram usadas portas alternativas no host (`5544`, `5673`, `15673`) para não
colidir com instalações locais; as portas internas permaneceram as padrão.

Essas observações são evidência funcional de execução e persistência, não
benchmark.

## Limitações atuais

- PostgreSQL e RabbitMQ continuam instâncias únicas;
- healthcheck de publisher/worker valida acesso às dependências; travamentos
  lógicos específicos serão cobertos pelos testes de falha da Etapa 5;
- `restart: unless-stopped` recupera processo, mas não substitui replicação ou
  alta disponibilidade;
- o fluxo de falha antes/depois de commit ainda precisa dos failpoints da
  Etapa 5.

