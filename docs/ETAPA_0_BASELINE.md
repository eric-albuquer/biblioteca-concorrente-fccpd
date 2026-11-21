# Etapa 0 — Auditoria e baseline

Data da execução: 2026-11-21.

## Estado inicial

- SHA: `1bd1553b4f79df124770f3f3dee589530b70de5f`.
- Branch inicial: `main`; desenvolvimento iniciado em
  `desenvolvimento/parte-2`.
- O SHA é exatamente o commit usado pelo plano, sem diferenças rastreadas.
- O único arquivo local anterior à implementação era
  `PLANO_IMPLEMENTACAO_PARTE_2.md`, não rastreado.
- Python disponível: 3.13.4 (também há 3.11 instalado).
- Docker CLI 29.6.1 e Compose 5.2.0 disponíveis; daemon parado durante a
  auditoria.
- `pytest` não estava instalado no ambiente inicial.

## Baseline executado

Servidor:

```powershell
py -3.13 test/servidor_test.py
```

Cliente, com UTF-8 habilitado para o console do Windows:

```powershell
$env:PYTHONUTF8='1'
py -3.13 test/cliente_test.py
```

Resultado observado: 100 clientes, 100 sucessos, 0 falhas, duração de 6,63 s;
91 clientes passaram pela fila. Ao final, os estoques voltaram a 3/3, 2/2 e
4/4 e as filas ficaram vazias. Sem `PYTHONUTF8=1`, a primeira execução concluiu
a lógica mas falhou ao imprimir o caractere `✅` no console CP-1252.

## Limitações confirmadas

- estoque, fila e empréstimos desaparecem com o processo;
- devolução por título não verifica posse e pode inflar o contador;
- cliente permanece bloqueado no socket durante toda a espera;
- não há identificador persistente, idempotência, banco ou mensageria;
- `monitorar_estoques` usa o comando Unix `clear`, inexistente no Windows.

## Checklist concreto da Etapa 1

- criar DDL idempotente com constraints, índices e relacionamentos;
- criar seed 3/2/4 para Duna, 1984 e O Hobbit;
- centralizar configuração/conexões PostgreSQL;
- registrar ordem FIFO sob lock da linha do livro;
- processar empréstimos com estoque e status na mesma transação;
- devolver por empréstimo e usuário, transferindo diretamente à fila;
- testar banco real, disputa concorrente, FIFO, duplicidade e devolução
  inválida;
- preservar os módulos V1 até a migração do Gateway na Etapa 2.
