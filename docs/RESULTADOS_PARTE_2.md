# Resultados e evidências — Parte 2

Data da medição: 25/11/2026  
Branch: `versao-simplificada`

## Testes funcionais e de consistência

A suíte executada contra PostgreSQL real terminou com **7/7 testes aprovados**.
Ela cobre contrato de mensagens, idempotência, posse na devolução, devolução
duplicada, estoque, FIFO persistida e duas transações simultâneas disputando o
último exemplar. Nesse conflito, o resultado foi exatamente um `EMPRESTADO`, um
`NA_FILA`, zero exemplares livres, um empréstimo ativo e uma espera.

O smoke test atravessou cliente, RabbitMQ, worker, PostgreSQL e fila de resposta:

1. empréstimo retornou `EMPRESTADO`;
2. devolução pelo dono retornou `DEVOLVIDO`;
3. segunda devolução retornou `ERRO` e não alterou estoque.

## Mensageria e falhas

- Queda injetada no worker 2 depois do commit e antes do ack.
- O log do worker 1 registrou o mesmo `request_id` com `redelivered=True`.
- O cliente recebeu o empréstimo uma única vez e conseguiu devolvê-lo.
- O worker 2 reiniciou automaticamente por `restart: unless-stopped`.
- Um cliente desconectou após publicar e recuperou a resposta ao reconectar.
- Uma resposta aguardou durante 2 segundos de consumidor lento.
- Um JSON inválido chegou à DLQ com a mensagem de validação.

Scripts reproduzíveis: `scripts/smoke_test.py`, `scripts/failure_demo.py` e
`scripts/messaging_resilience_demo.py`.

## Benchmark da tarefa paralela

Tarefa: analisar históricos de empréstimos separados nas seis categorias,
calculando empréstimos, atrasos, dias de atraso, usuários únicos e livros mais
procurados. Dataset determinístico com seed fixa, 600.000 eventos, uma execução
de aquecimento e cinco repetições medidas por configuração.

Ambiente: Windows 11, Python 3.13.4, CPU Intel64 Family 6 Model 183, 32 CPUs
lógicas e 34.049.417.216 bytes de memória física. Todos os modos produziram o
mesmo SHA-256:
`c40f4600ceeccda172f4e40a80e5f3e6bbd4b1f1b3ceb0800fede2911abf5365`.

| Modo | Processos | Mediana (s) | p95 (s) | Desvio (s) | Speedup | Eficiência |
|---|---:|---:|---:|---:|---:|---:|
| Sequencial | 0 | 1,6024 | 1,6107 | 0,0390 | 1,00× | 100,0% |
| Paralelo | 1 | 1,8402 | 1,8885 | 0,0657 | 0,87× | 87,1% |
| Paralelo | 2 | 1,0469 | 1,1079 | 0,0646 | 1,53× | 76,5% |
| Paralelo | 4 | 0,9615 | 1,0512 | 0,1066 | 1,67× | 41,7% |
| Paralelo | 6 | 0,7153 | 1,1664 | 0,2504 | 2,24× | 37,3% |

Interpretação: um único processo no `ProcessPoolExecutor` perde para o laço
sequencial devido à criação do processo e IPC. Dois ou mais processos trazem
ganho real. Seis foram mais rápidos porque há exatamente seis partições, mas a
eficiência cai: leitura dos arquivos, criação/coordenação de processos e partes
não paralelizáveis limitam a escala. O p95 maior com seis também mostra mais
variabilidade de escalonamento, portanto não é correto projetar ganho linear.

Dados brutos: [`benchmark-results/raw.csv`](../benchmark-results/raw.csv) e
[`benchmark-results/summary.json`](../benchmark-results/summary.json).

## Benchmark ponta a ponta

Cada repetição publicou 1.200 comandos por oito produtores paralelos e mediu o
caminho RabbitMQ → worker → transação PostgreSQL → RabbitMQ → cliente. Foram
cinco repetições com 1, 2, 4 e 6 workers. O comando de devolução de empréstimo
inexistente percorre validação, claim idempotente, consulta transacional,
persistência da resposta de domínio e publisher confirm, sem alterar estoque.

Ambiente adicional: Docker 29.6.1 e Docker Compose 5.2.0.

| Workers | Vazão mediana (cmd/s) | Latência mediana (ms) | p95 (ms) | Fila máxima |
|---:|---:|---:|---:|---:|
| 1 | 288,47 | 267,53 | 496,17 | 84 |
| 2 | 535,73 | 27,13 | 42,58 | 12 |
| 4 | 860,35 | 15,19 | 25,59 | 10 |
| 6 | 982,05 | 11,55 | 22,68 | 5 |

Interpretação: instâncias adicionais reduzem backlog e latência e elevam a
vazão. O ganho de 1 para 2 é grande; de 4 para 6 é menor, sinal de saturação por
PostgreSQL, RabbitMQ, publisher confirms e coordenação dos produtores. Como os
workers compartilham banco e broker, a arquitetura não promete escala linear.
O primeiro cenário também exibiu aquecimento mais variável; por isso a tabela
usa mediana de cinco repetições e preserva todas as amostras.

Dados brutos: [`benchmark-e2e-results/raw.csv`](../benchmark-e2e-results/raw.csv)
e [`benchmark-e2e-results/summary.json`](../benchmark-e2e-results/summary.json).

## Comandos usados

```powershell
$env:PYTHONPATH="$PWD\src;$PWD"
$env:TEST_DATABASE_URL="postgresql://biblioteca:biblioteca@localhost:15432/biblioteca"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m benchmarks.run_parallel_benchmark
.\.venv\Scripts\python.exe -m benchmarks.run_e2e_benchmark
docker compose run --build --rm client python scripts/smoke_test.py
docker compose run --build --rm client python scripts/failure_demo.py
docker compose run --build --rm client python scripts/messaging_resilience_demo.py
```

Os números são uma observação desta máquina, não garantia para outros ambientes.
