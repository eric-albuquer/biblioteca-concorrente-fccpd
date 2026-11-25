# Guia de aprendizado e apresentação — Parte 2

## Roteiro de 10 minutos

| Tempo | Tema | Demonstração sugerida |
|---:|---|---|
| 0:00–1:00 | Problema e evolução da Parte 1 | Mostrar a espera FIFO e a limitação single-node |
| 1:00–2:30 | Arquitetura | Explicar clientes, RabbitMQ, workers e PostgreSQL no diagrama |
| 2:30–4:00 | Mensageria | Mostrar seis filas, respostas por cliente, ack, retry e DLQ |
| 4:00–5:30 | Consistência | Rodar teste simultâneo do último exemplar e explicar o lock de linha |
| 5:30–7:00 | Falha | Rodar `failure_demo.py` e apontar `redelivered=True` nos logs |
| 7:00–8:30 | Paralelismo | Mostrar algoritmo, igualdade de hash, speedup e queda de eficiência |
| 8:30–9:30 | Benchmark distribuído | Comparar vazão, p95 e backlog com 1/2/4/6 workers |
| 9:30–10:00 | IA e limitações | Mostrar prompts/decisões e declarar o backlog pós-entrega |

Dividam os blocos entre os quatro integrantes, mas façam revisão cruzada: a
rubrica exige que todos consigam responder sobre o sistema inteiro.

## Perguntas que todos devem saber responder

### Por que seis filas, e não uma por livro?

As seis categorias dão isolamento suficiente e permitem roteamento simples.
Milhares de filas consumiriam recursos e não evitariam a falha total do broker.
Uma mensagem defeituosa é isolada por retry/DLQ, não por uma fila exclusiva.

### Por que não usar Single Active Consumer?

Ele serializaria todos os livros da categoria. Os workers podem competir porque
o banco serializa apenas o mesmo livro e mantém a FIFO da espera em transação.

### O que impede dois clientes de pegarem o último exemplar?

`SELECT ... FOR UPDATE` bloqueia a linha do livro. A segunda transação só lê o
estado depois do commit da primeira. A constraint do estoque é uma segunda
barreira contra valores inválidos.

### Por que o ack vem no fim?

Se o ack viesse antes do commit/resposta, uma queda perderia o pedido. Com ack
manual no fim, a conexão quebrada devolve a mensagem à fila.

### E se a queda ocorrer depois do commit?

O `request_id` já está em `processed_requests` junto às respostas. O próximo
worker não repete a operação: ele republica o resultado persistido e confirma a
mensagem.

### Por que o cliente pode reconectar?

A fila `biblioteca.responses.<client_id>` é durável. O mesmo ID volta a consumir
eventos que chegaram durante a desconexão. Eventos têm ID determinístico e o
cliente deduplica durante a sessão.

### Como a espera FIFO sobrevive a falhas?

Ela é uma tabela PostgreSQL ordenada pelo ID crescente. O pedido recebe
`NA_FILA`; uma devolução bloqueia o livro, escolhe o primeiro `WAITING`, cria o
empréstimo e emite `EMPRESTIMO_LIBERADO` na mesma transação lógica.

### Por que processos no relatório analítico?

É uma tarefa de CPU e parsing. Processos permitem execução simultânea em mais de
um núcleo; threads Python não são a escolha adequada para demonstrar esse ganho.

### Por que seis processos não dão speedup 6×?

Há criação de processos, leitura de disco, IPC, agregação e partes sequenciais.
Por isso o speedup observado foi 2,24× e a eficiência caiu para 37,3%.

### O que ainda não é tolerante a falhas?

O consumidor é redundante, mas RabbitMQ e PostgreSQL têm uma instância no
Compose. Cluster/quorum queues e replicação do banco ficaram conscientemente
fora do escopo desta entrega.

## Checklist antes da apresentação

- executar `docker compose up -d --build` numa máquina limpa;
- deixar RabbitMQ Management aberto em <http://localhost:15672>;
- rodar os 7 testes, smoke, resiliência e falha;
- confirmar nos logs a queda, `redelivered=True` e reinício;
- não apagar os resultados brutos do benchmark;
- cada integrante explicar um trecho que não implementou diretamente;
- ter capturas dos resultados como contingência, sem substituir a demo ao vivo;
- declarar limitações com clareza, sem prometer cluster que não foi implementado.
