# Etapa 1 — Banco e regras transacionais

Esta etapa introduz o PostgreSQL como fonte de verdade sem remover o protótipo
V1. O Gateway TCP ainda será migrado na Etapa 2; por enquanto, as regras podem
ser exercitadas diretamente por `ServicoEmprestimos` e pelos testes.

## Modelo e garantias

- `livros`: capacidade fixa, disponibilidade limitada por `CHECK` e contador
  `ultimo_seq_pedido` por título;
- `usuarios`: identidade usada para validar a posse;
- `solicitacoes`: UUID idempotente, tipo, status e `ordem_fila` única por livro;
- `emprestimos`: uma linha por pedido de empréstimo, com
  `borrow_request_id UNIQUE`;
- `outbox`: estrutura criada para a aceitação durável da Etapa 2.

A aceitação de um pedido bloqueia a linha de `livros`, incrementa a sequência e
grava a solicitação na mesma transação. O processamento também bloqueia essa
linha e busca todos os estados `RECEBIDO`/`AGUARDANDO` ordenados por
`ordem_fila`; a mensagem que disparou o processamento não pode furar a fila.

Na devolução, a ordem de locks é sempre livro e depois empréstimo. Se houver
fila, o empréstimo é encerrado e uma nova linha é criada para o primeiro pedido
na mesma transação, sem incrementar `disponiveis`. Sem fila, a disponibilidade
é incrementada sob a constraint de capacidade.

## Configuração local

Crie o ambiente e instale as dependências:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

Configure um PostgreSQL de teste vazio. Exemplo usado nesta etapa:

```powershell
docker run --rm -d --name biblioteca-parte2-postgres-test `
  -e POSTGRES_DB=biblioteca_test `
  -e POSTGRES_USER=biblioteca `
  -e POSTGRES_PASSWORD=biblioteca_test `
  -p 127.0.0.1:5544:5432 postgres:16-alpine

$env:TEST_DATABASE_URL = `
  'postgresql://biblioteca:biblioteca_test@127.0.0.1:5544/biblioteca_test'
.\.venv\Scripts\python.exe -m pytest -v
```

Os testes PostgreSQL são ignorados quando `TEST_DATABASE_URL` não existe; isso
evita usar acidentalmente uma base não indicada. A validação de aceite exige
rodá-los com uma base real, como no comando acima.

Para aplicar somente o schema e o seed por Python:

```powershell
$env:DATABASE_URL = 'postgresql://usuario:senha@host:5432/biblioteca'
.\.venv\Scripts\python.exe -c `
  "from database.connection import aplicar_schema; aplicar_schema(incluir_seed=True)"
```

## Cobertura automatizada desta etapa

- catálogo e capacidades do seed 3/2/4;
- reaplicação segura de schema e seed;
- rejeição de disponibilidade negativa ou acima da capacidade;
- unicidade de ordem FIFO e `borrow_request_id`;
- oito threads concorrendo por um exemplar;
- entrega fora de ordem que ainda atende o pedido persistido mais antigo;
- transferência da unidade por toda a fila sem dupla contagem;
- devolução por outro usuário e segunda devolução rejeitadas;
- retransmissão idempotente e conflito do mesmo UUID com outro payload.

