# Etapa 2 — Gateway V2, idempotência e outbox

O Gateway V2 preserva o cabeçalho TCP binário `!BI`, mas substitui os payloads
textuais da Parte 1 por objetos JSON UTF-8. Ele aceita comandos de forma durável
no PostgreSQL e não altera estoque nem empréstimos; essa aplicação assíncrona
será responsabilidade dos workers da Etapa 3.

## Fluxo de aceitação

Para `PEDIR`, uma única transação:

1. serializa retransmissões do mesmo UUID com advisory lock;
2. valida usuário e livro;
3. bloqueia a linha do livro e reserva `ordem_fila`;
4. insere `solicitacoes` com estado `RECEBIDO`;
5. insere o evento JSON correspondente em `outbox`;
6. confirma a transação e só então responde ao cliente.

Para `DEVOLVER`, o Gateway valida que usuário e empréstimo existem, descobre o
livro e grava solicitação + outbox. Posse, estado ativo e transferência FIFO são
regras do worker: o aceite não marca o empréstimo como devolvido.

Se a inserção na outbox falhar, toda a transação é revertida, inclusive a ordem
FIFO reservada. Uma retransmissão com o mesmo UUID e payload devolve o estado já
existente; o mesmo UUID com outro comando ou dados retorna
`REQUEST_ID_CONFLITANTE`.

## Contrato TCP V2

Cabeçalho mantido:

```text
!BI = operação (1 byte) + tamanho do payload (4 bytes, big-endian)
```

Operações:

| Código | Nome | Payload |
|---:|---|---|
| 1 | `PEDIR` | `request_id`, `usuario_id`, `livro_id` |
| 2 | `DEVOLVER` | `request_id`, `usuario_id`, `emprestimo_id` |
| 3 | `RESPOSTA` | objeto com solicitação/status |
| 4 | `ERRO` | `erro` e `mensagem` |
| 5 | `STATUS` | `request_id` |

Exemplo de pedido:

```json
{
  "request_id": "7e1be25e-81da-4bc9-9e3d-5292e9d7d401",
  "usuario_id": 1,
  "livro_id": 1
}
```

Resposta com worker desligado:

```json
{
  "request_id": "7e1be25e-81da-4bc9-9e3d-5292e9d7d401",
  "status": "RECEBIDO",
  "tipo": "PEDIR",
  "usuario_id": 1,
  "livro_id": 1,
  "emprestimo_id": null,
  "ordem_fila": 1,
  "erro": null
}
```

O limite é 64 KiB por payload. JSON inválido, campos ausentes/extras, UUID
inválido e identificadores que não sejam inteiros positivos são rejeitados com
erro estruturado.

## Ruptura V1 → V2

A mudança é intencionalmente incompatível:

- `pedir_livro("Duna")` foi substituído por
  `pedir_livro(usuario_id, livro_id, request_id=None)`;
- `devolver_livro("Duna")` foi substituído por
  `devolver_livro(usuario_id, emprestimo_id, request_id=None)`;
- o resultado inicial não é mais a ficha final: é uma solicitação estruturada
  em `RECEBIDO`;
- `consultar_status(request_id)` recupera o andamento após reconexão;
- o cliente gera UUID quando necessário e reutiliza o mesmo payload nas
  tentativas automáticas de transporte.

## Execução desta etapa

Com PostgreSQL configurado e sem publisher/worker:

```powershell
$env:DATABASE_URL = 'postgresql://usuario:senha@localhost:5432/biblioteca'
.\.venv\Scripts\python.exe test/servidor_test.py
```

Em outro terminal:

```powershell
.\.venv\Scripts\python.exe test/cliente_test.py 100
```

Para testes automatizados:

```powershell
$env:TEST_DATABASE_URL = `
  'postgresql://usuario:senha@localhost:5432/biblioteca_test'
.\.venv\Scripts\python.exe -m pytest -v
```

## Evidências observadas em 2026-11-21

- suíte completa: 30 testes aprovados em 3,57 s com PostgreSQL 16 real;
- 100 clientes V2: 100 aceites e 100 consultas, 0 erros, 0,68 s;
- latência média conjunta de aceite + status: 411,72 ms nessa execução;
- todos permaneceram `RECEBIDO`, como esperado com worker desligado;
- reinício do Gateway não apagou a solicitação e a consulta por UUID continuou
  funcionando;
- oito retransmissões concorrentes produziram uma solicitação e um evento;
- uma queda controlada após o envio provou que o retry reutiliza o mesmo UUID;
- falha controlada na outbox reverteu solicitação e sequência FIFO.

Esses números são evidência funcional dessa execução local, não benchmark; a
metodologia de desempenho será implementada somente na Etapa 6.

## Limitações atuais

- não há publicação no RabbitMQ nem consumo: são responsabilidades da Etapa 3;
- solicitações permanecem em `RECEBIDO` enquanto nenhum worker as processa;
- PostgreSQL indisponível impede o aceite durável e gera
  `BANCO_INDISPONIVEL`;
- os diagramas completos da arquitetura distribuída serão consolidados nas
  etapas posteriores.

