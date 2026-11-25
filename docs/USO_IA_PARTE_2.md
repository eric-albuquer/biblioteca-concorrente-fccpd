# Relatório de uso de IA — Parte 2

Este relatório é atualizado durante toda a Parte 2. Ele registra pedidos,
sugestões, verificações e decisões; não substitui a revisão da equipe.

## Registro 001 — análise inicial e desenho da solução

Data: 25/11/2026
Ferramenta: ChatGPT/Codex
Responsável pela decisão final: equipe do projeto

### Pedido relevante

A equipe solicitou análise completa da `main`, do `RULES.md` e da rubrica;
avaliação de uma arquitetura na qual clientes publicam pedidos, RabbitMQ separa
filas por categoria, um serviço com banco próprio processa os comandos e publica
respostas; sugestões simples que cubram todos os requisitos; e documentação de
tudo antes da implementação.

#### Prompt exato da equipe

```text
nos vamos fazer a implementação da parte 2 desse projeto.
no RULES.md esta o comanda do que foi passado pelo professor e deve ser feito. vamos no basear nisso para desenvolver o projeto de maneira simples e que atenda a todos os requisitos e todas as rubricas (esta na imagem). documente tudo que foi feito, sugestões dadas e decisões feitas.

primeiramente analise o projeto atual por completo (main), o rules.md, veja o que temos e o que atende aos requisitos e documente.

a arquitetura do projeto sera o seguinte: teremos os clientes que serão responsáveis por por fazer as requisiçoes de livros, pedir, devolver etc (productor). essas requisições serão mandadas para uma fila RabbitMQ que irá guardar as requisições e procesa-las para um microserviço que contem seu proprio banco de dados que sera responsável por processar as requisições da fila(consumer). as filas do rabbitmq serão por categorias de livros (ficção, romance etc ). o micro serviço deve retornar a uma outra fila(producer) que ira retornar ao cliente. o sistema deve ser distribuido.

tendo em vista essa arquitetura projetada, analise a possibilidade de implementação, de preferencia de maneira simples, mas que atenda a todos os requisitos do projeto e que está no rules.md, faça sugestões de melhorias e documente. para em seguida fazer-mos a implementação
```

### Ações realizadas pela IA

- inventariou todos os arquivos rastreados da `main`;
- confirmou o commit analisado e o estado inicial do Git;
- leu integralmente código, README, arquitetura, regras e os títulos visíveis
  da rubrica;
- compilou os módulos;
- verificou que `unittest` não descobre testes;
- executou o fluxo TCP com 30 clientes;
- reproduziu a devolução que viola a capacidade do estoque;
- identificou problemas de portabilidade no Windows;
- consultou documentação oficial do RabbitMQ e PostgreSQL;
- produziu a análise, o plano e o registro de decisões.

### Resultados observados

- compilação: sucesso;
- teste TCP em UTF-8: 30 clientes atendidos, 0 falhas, 21 passaram pela fila,
  saída 0;
- teste no console padrão do Windows: falha de codificação ao imprimir emoji;
- servidor no Windows: comando `clear` inexistente;
- descoberta `unittest`: 0 testes;
- devolução sem posse: `available=2` para capacidade `1`.
- rubrica inicial: sete critérios estavam visíveis e somavam 28 pontos, mas o
  cabeçalho informava 32; a questão foi resolvida no Registro 002.

### Sugestões aceitas como diretriz do usuário

- RabbitMQ como broker;
- clientes como produtores;
- filas por categoria;
- serviço consumidor com banco próprio;
- resposta devolvida por mensageria;
- execução distribuída;
- preferência por solução simples.

### Sugestões que estavam pendentes naquele momento

- PostgreSQL com consistência forte e `SELECT ... FOR UPDATE`;
- idempotência por `request_id`;
- ack manual após commit e publisher confirm;
- fila de resposta por cliente;
- fila de espera persistida no banco;
- dois workers e Single Active Consumer por categoria quando a ordem fosse
  necessária — esta alternativa foi posteriormente substituída pela recomendação
  de competing consumers no Registro 003;
- retry limitado e DLQ;
- relatório analítico por categoria com versão sequencial e paralela;
- Docker Compose e teste de falha derrubando o worker ativo.

### Sugestões rejeitadas ou adiadas nesta análise

- gateway adicional: adiado para manter o MVP simples;
- cluster RabbitMQ e PostgreSQL replicado: adiados por complexidade;
- muitos microsserviços: rejeitados para o MVP;
- mensagem de pedido mantida sem ack enquanto falta estoque: rejeitada;
- usar as threads atuais como prova de paralelismo: rejeitada;
- inventar ou estimar resultados do benchmark: rejeitado.

### Decisões que ainda dependiam da equipe naquele momento

- confirmar se a branch remota existente será auditada/reaproveitada;
- aprovar categorias e tarefa pesada do benchmark;
- revisar nomes, regras de negócio e escopo da apresentação;
- executar novamente todos os testes e benchmarks no ambiente oficial;
- aceitar ou rejeitar formalmente as propostas marcadas como pendentes.

O Registro 003 resolve a escolha da `main`, categorias, fila de espera, tarefa
pesada e quase todas as propostas. Somente a recomendação final das decisões
D-004 e D-012 aguarda confirmação da equipe.

### Código produzido

Nenhuma funcionalidade da Parte 2 foi implementada neste registro. As únicas
alterações foram documentos de análise e planejamento.

### Referências consultadas

- [RabbitMQ — Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ — Reliability Guide](https://www.rabbitmq.com/docs/reliability)
- [RabbitMQ — Consumers e Single Active Consumer](https://www.rabbitmq.com/docs/consumers)
- [RabbitMQ — Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ — Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
- [PostgreSQL — Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html)

## Registro 002 — revisão com a rubrica detalhada

Data: 25/11/2026
Ferramenta: ChatGPT/Codex
Tipo de mudança: documentação e critérios de aceitação

### Prompt exato da equipe

```text
coloquei as especificações das rubricas, refaça a analise e se necessario, altere levando em consideração as novas informações
```

### Novas evidências fornecidas

Foram adicionadas quatro imagens com os níveis Exceeding, Meeting, Approaching
e Far from approaching. Elas confirmaram oito critérios de 4 pontos e revelaram
o critério que não estava visível anteriormente: **Oral Presentation and Team
Collaboration**.

### Análise e alterações feitas pela IA

- corrigiu a documentação de sete para oito critérios e total de 32 pontos;
- transformou cada nível Exceeding em evidência objetiva de conclusão;
- manteve a arquitetura RabbitMQ + workers + PostgreSQL, pois ela continua
  adequada à rubrica;
- ampliou o benchmark paralelo para várias contagens de processos, speedup,
  eficiência e ponto de saturação;
- tornou obrigatório testar consumidor lento e consumidor interrompido;
- esclareceu que o worker standby deve assumir automaticamente, sem reinício ou
  outra intervenção manual;
- acrescentou um roteiro de apresentação de 10 minutos com fala de toda a
  equipe e perguntas cruzadas;
- passou a registrar os prompts exatos neste diário.

### Sugestões mantidas

- entrega pelo menos uma vez com idempotência;
- ack depois do commit e do publisher confirm;
- consistência forte no PostgreSQL;
- duas instâncias do serviço consumidor;
- fila de espera persistida;
- relatório analítico por categoria como tarefa paralela real;
- Docker Compose para execução e demonstração.

### Sugestões alteradas pela nova rubrica

- **Antes:** comparar apenas sequencial e paralelo.  
  **Agora:** medir várias contagens de processos e explicar onde o ganho deixa
  de crescer.
- **Antes:** demonstrar redelivery após derrubar um worker.  
  **Agora:** provar recuperação automática, sem passo manual, e incluir também
  consumidor lento.
- **Antes:** preparar uma apresentação oral genérica.  
  **Agora:** ensaiar 10 minutos, garantir fala de todos e preparar todos para
  responder sobre o sistema inteiro.
- **Antes:** resumir o pedido no diário de IA.
  **Agora:** preservar o prompt exato, ligar sugestões a mudanças e registrar
  correções e decisões.

### Código produzido

Nenhuma funcionalidade foi implementada nesta revisão. Foram alterados somente
os documentos de análise, decisões e uso de IA.

## Registro 003 — consolidação das decisões da equipe

Data: 25/11/2026  
Ferramenta: ChatGPT/Codex  
Tipo de mudança: decisões arquiteturais, falha de workers e correção de posse

### Prompt exato da equipe

```text
ANALISE E PLANO:
a branch de desenvolvimento parte 2 não deve ser considerada.
não sera revisada, ou reutilizada, o trabalho partirá exclusivamente da main.
devemos corrigir essa falha de duplicidade se devolver mmais de um livro disponivel, devendo ter verificação de posse.
temos que levar em consideração também as falhas dos workers, caso eles caiam, o que acontece comm os pedidos.
DECISOES:
001 - aprovado
002 - aprovado
003 - aprovado
004 - analisar a possivel queda de uma fila de uma categoria inteira e se a latencia de ter milhares de filas vale a pena para garantir que se um livro de determinada categoria cair, não afete os outros livros da categoria
005 - aprovada mas analisar a possibilidade de mais para o benchmark
006 - aprovado
007 - aprovado
008 - aprovado
009 - aprovado
010 - aprovado
011 - aprovado
012 - analisar conforme a resposta da 004
013 - aprovado
014 - aprovado conforme as versões anteriores
015 - aprovado
016 - aprovado
017 - apenas apos a execução e analise
018 - na considere a branch de desenvolvimento parte 2 para nada
019 - objetivo é chegar na 4/4
020 - aprovado
021 - aprovado
022 - apos toda a implementação, faremos  um documento para aprendizado e apresentação
023 - aprovado
as sugestões serão feitas apos a segunda entrega ou em um periodo posterior, vamos dar prioridade as implementações neccessarias para a rubrica e manter a simplicidade do projeto para que seja facil de entender e de aprender com ele

em relação as categorias fixas, vai depender da analise anterior, mmas caso fomos manter por categorias, coloque ficção científica, romance, aventura, ação, suspense e terror.

a principio o cliente deveria permancer inscrito, mas devemos suportar ambos para minimizar e tratar as falhas e a fila de espera idealmente deve também ser persistida no banco de dados por garantia.

e quanto a tarefa pesada do dominio sim.

USO IA:

continue com a documentação mostrando os prompts, decisões feitas e o porque.
```

### Decisões confirmadas

- implementação exclusivamente a partir da `main`; branches anteriores de
  Parte 2 não serão consultadas nem reutilizadas;
- checkout local alterado para `main`, que já apontava para o mesmo commit-base;
- RabbitMQ, clientes produtores/consumidores de resposta, PostgreSQL próprio do
  serviço, consistência forte, idempotência, ack manual, espera persistente,
  retry/DLQ, Docker Compose e demonstração de falha foram aprovados;
- o objetivo é 4/4 nos oito critérios;
- o relatório analítico por categoria foi confirmado como tarefa pesada;
- melhorias não necessárias à rubrica foram movidas para depois da entrega;
- o documento de aprendizado/apresentação será produzido após a implementação.

### Análise solicitada sobre as filas

A IA recomendou seis filas fixas, uma para cada categoria aprovada, e rejeitou
uma fila por livro. Milhares de filas aumentariam recursos, bindings, métricas e
configuração sem isolar uma queda do broker. Uma falha de processamento deve ser
isolada por mensagem com retry limitado e DLQ. Dois ou mais competing consumers
com prefetch pequeno permitem que outro pedido da categoria avance.

Como a ordem e as invariantes serão mantidas no PostgreSQL, a IA recomendou não
usar Single Active Consumer inicialmente: ele serializaria livros diferentes da
mesma categoria. Se o benchmark ou uma exigência de ordenação mostrar o
contrário, essa decisão pode ser revista com evidência.

Categorias registradas: ficção científica, romance, aventura, ação, suspense e
terror.

### Tratamento definido para queda de worker

- mensagem sem ack retorna automaticamente à fila quando a conexão fecha;
- queda antes do commit reverte a transação e outro worker processa o pedido;
- queda depois do commit usa `processed_requests` para não repetir o efeito;
- resposta duplicada é deduplicada pelo cliente por `request_id`;
- se todos os workers caírem, pedidos persistentes aguardam até um voltar;
- a demonstração usará ao menos dois workers e recuperação sem intervenção.

### Correção definida para devolução indevida

A devolução passará a usar `loan_id` e `client_id`. O serviço só altera estoque
quando o empréstimo existe, pertence ao cliente e está ativo. Devolução repetida,
sem posse ou de outro cliente será rejeitada em transação, e uma constraint
impedirá `available_copies` de ultrapassar `total_copies`.

### Cliente e fila de espera

O cliente permanece inscrito normalmente, mas a fila de resposta durável e o
`client_id` estável permitirão reconexão. A espera FIFO será persistida no
PostgreSQL para sobreviver à queda de cliente, worker ou processo.

### Arquivos alterados

- `docs/ANALISE_E_PLANO_PARTE_2.md`;
- `docs/DECISOES_PARTE_2.md`;
- `docs/USO_IA_PARTE_2.md`.

### Código produzido

Nenhuma funcionalidade foi implementada neste registro. As alterações foram
somente de análise e documentação.

### Referências consultadas nesta decisão

- [RabbitMQ — Queues](https://www.rabbitmq.com/docs/queues)
- [RabbitMQ — Consumer Acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ — Consumers](https://www.rabbitmq.com/docs/consumers)
- [RabbitMQ — Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)

## Padrão obrigatório para os próximos registros de IA

Cada uso relevante de IA durante a implementação deverá registrar:

1. prompt exato;
2. sugestão ou código recebido;
3. arquivos e trechos afetados;
4. o que a equipe aceitou, corrigiu ou rejeitou;
5. justificativa da decisão;
6. testes executados e resultados reais;
7. explicação humana do comportamento implementado.

O relatório comprova o processo, mas o critério máximo também exige que os
integrantes entendam o código. Por isso, cada módulo implementado com auxílio de
IA deverá entrar no roteiro de revisão cruzada da equipe antes da apresentação.

## Registro 004 — implementação por etapas e validação completa

Data: 25/11/2026  
Ferramenta: ChatGPT/Codex  
Tipo de mudança: implementação, testes, benchmark e documentação

### Prompt exato da equipe

```text
vamos manter o desenvolvimento na branch: versao-simplificada e so vamos dar o merge apos o funcionamento pleno do programa.

apos a analise vamos também, manter pelas categorias antes citadas e não por filas individuais de livros. 

em relação ao benchmark vamos adotar a tarefa proposta e metodologia planejada.

004 - aprovada
012 - aprovada
017 - aprovada
018 - desenvolver na branch versao-simplificada

comece a implementação pelas etapas 
```

Após uma interrupção da sessão, a equipe enviou o prompt exato:

```text
continue
```

Esse segundo prompt foi interpretado somente como continuação do trabalho já
autorizado, sem mudança de escopo ou decisão.

### Implementação sugerida e aceita

- contrato JSON versionado para `PEDIR`, `DEVOLVER` e respostas;
- seis filas de comandos, seis filas de retry, DLQ e filas duráveis por cliente;
- clientes produtores/consumidores, dois workers concorrentes e PostgreSQL;
- transações, lock de linha, constraints, idempotência e fila FIFO persistida;
- ack manual depois de commit e publisher confirm;
- correção de posse por `loan_id` e `client_id`;
- Docker Compose, migração e seed de seis livros;
- testes unitários/integrados, smoke, cliente lento/desconectado, DLQ e queda de
  worker após commit;
- tarefa analítica paralela e benchmark ponta a ponta com 1/2/4/6 workers;
- arquitetura, resultados e material de aprendizado/apresentação.

### Decisões e correções durante a implementação

| Situação | Decisão | Por quê |
|---|---|---|
| PostgreSQL nativo já usava `localhost:5432` | Compose expõe o banco em `15432` | Evita alcançar o banco errado sem apagar ou modificar a instalação do usuário |
| `55432` estava em uma faixa reservada pelo Windows | Rejeitada e substituída por `15432` | O Docker não conseguiu fazer o bind; as faixas foram consultadas antes da nova escolha |
| Lista/dicionário enviados a uma coluna JSONB | Usar `psycopg.types.json.Jsonb` | Torna a adaptação explícita e portável no Psycopg 3 |
| Resultados ainda não mediam conflito simultâneo | Adicionar teste com `Barrier` e duas conexões | Demonstra de fato a consistência sob concorrência, em vez de apenas sequencialmente |
| Benchmark inicial tinha somente mediana | Adicionar p95, desvio, memória, CSV/JSON/Markdown | Cumpre a metodologia aprovada e permite interpretar variabilidade |
| Faltava medir a arquitetura distribuída | Adicionar benchmark com oito produtores e 1/2/4/6 workers | Mede vazão, latência e profundidade das filas sem substituir a tarefa paralela de CPU |
| Resultados gerados estavam ignorados pelo Git | Manter apenas o dataset grande ignorado | CSV, JSON e resumo são pequenos e devem acompanhar a entrega |

Não foram adicionados gateway, cluster RabbitMQ, replicação PostgreSQL ou novos
microsserviços. Essas sugestões continuam adiadas conforme decisão da equipe.

### Arquivos principais produzidos

- `compose.yaml`, `Dockerfile`, `requirements.txt` e `.dockerignore`;
- `src/biblioteca_distribuida/` com configuração, contrato, mensageria, cliente,
  worker, serviço e migração;
- `migrations/001_initial.sql`;
- `tests/unit/` e `tests/integration/`;
- `scripts/smoke_test.py`, `scripts/failure_demo.py` e
  `scripts/messaging_resilience_demo.py`;
- `benchmarks/` e os dois diretórios de resultados;
- documentação da arquitetura, resultados e apresentação.

### Testes e resultados reais

- compilação de `src`, `tests`, `scripts` e `benchmarks`: sucesso;
- suíte final: 7/7 testes aprovados;
- smoke distribuído: sucesso;
- queda após commit: worker 2 encerrou, worker 1 registrou
  `redelivered=True`, resposta recuperada sem empréstimo duplicado;
- reconexão/consumidor lento/DLQ: sucesso;
- benchmark de CPU: 600.000 eventos, 5 repetições, hash idêntico, speedup máximo
  observado de 2,24× com 6 processos;
- benchmark ponta a ponta: 1.200 comandos por repetição, 8 produtores, 5
  repetições; vazão mediana de 288,47 cmd/s com 1 worker e 982,05 cmd/s com 6.

Os resultados completos, ambiente e limitações estão em
`docs/RESULTADOS_PARTE_2.md`; não foram estimados nem escolhidos previamente.

### Explicação humana do comportamento

O broker guarda o comando até um worker confirmar. O worker abre uma transação,
registra o `request_id`, bloqueia a linha do livro, aplica a regra e persiste a
resposta. Só depois publica a resposta com confirmação e envia o ack. Assim, uma
queda antes do commit não deixa efeito e uma queda depois do commit não duplica
o efeito. O outro worker lê o resultado idempotente e conclui o fluxo. A fila de
espera deixa de ser uma thread bloqueada e passa a ser estado persistente, capaz
de sobreviver a reinícios e desconexões.
