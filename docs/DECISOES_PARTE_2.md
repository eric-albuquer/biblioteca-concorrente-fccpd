# Registro de decisões da Parte 2

Data inicial: 25/11/2026

Este arquivo é um registro vivo. “Diretriz confirmada” representa algo definido
pelo enunciado ou pelo grupo. “Proposta” ainda deve ser validada durante a
implementação. Mudanças devem manter a decisão anterior e acrescentar a nova
justificativa, preservando o histórico.

| ID | Decisão | Estado | Justificativa e consequência |
|---|---|---|---|
| D-001 | Usar RabbitMQ como broker | Aprovada pela equipe | Atende comunicação assíncrona e combina com o modelo produtor/consumidor proposto |
| D-002 | Clientes publicam comandos e consomem suas respostas diretamente | Aprovada pela equipe | É o caminho acadêmico mais simples e evita adicionar um gateway agora; credenciais do broker nos clientes são uma limitação conhecida |
| D-003 | Separar filas de comandos por categoria de livro | Aprovada pela equipe | Serão seis categorias fixas: ficção científica, romance, aventura, ação, suspense e terror |
| D-004 | Exchange `topic` para comandos, `direct` para respostas e seis filas de categoria | Aprovada e implementada | Uma fila por categoria equilibra isolamento e simplicidade; não haverá fila por livro. Retry/DLQ impede uma mensagem defeituosa de bloquear a categoria. Queda real da fila é falha de infraestrutura do broker e não é resolvida criando milhares de filas |
| D-005 | Um serviço lógico de empréstimos com duas instâncias no fluxo normal | Aprovada pela equipe | Cumpre failover sem microsserviços artificiais; o benchmark também medirá 1, 2, 4 e, se viável, 6 workers |
| D-006 | PostgreSQL é o banco exclusivo do serviço de empréstimos | Aprovada pela equipe | Clientes não acessam tabelas; todas as instâncias compartilham o banco do mesmo serviço lógico |
| D-007 | Consistência forte para estoque, empréstimo, devolução e fila | Aprovada pela equipe | Transações, locks de linha e constraints impedem empréstimo duplo e estoque inválido |
| D-008 | Entrega pelo menos uma vez com idempotência por `request_id` | Aprovada pela equipe | Redelivery é esperado em falhas; duplicata de transporte não pode virar duplicata de negócio |
| D-009 | Ack manual somente após commit e publisher confirm da resposta | Aprovada pela equipe | Evita perder comando confirmado cedo demais e permite recuperação do worker |
| D-010 | Persistir a fila de espera no PostgreSQL | Aprovada pela equipe | Preserva pedidos e sua ordem durante reinício ou desconexão |
| D-011 | Responder `NA_FILA` e depois emitir `EMPRESTIMO_LIBERADO` | Aprovada pela equipe | Adapta a espera bloqueante da Parte 1 ao modelo assíncrono sem perder a semântica FIFO |
| D-012 | Não usar Single Active Consumer no desenho inicial; usar competing consumers | Aprovada e implementada | SAC serializaria livros diferentes da mesma categoria. O banco garante consistência/FIFO da espera; consumidores concorrentes, prefetch pequeno e retry/DLQ reduzem bloqueio entre livros |
| D-013 | Retry com limite e DLQ para mensagens inválidas/venenosas | Aprovada pela equipe | Evita loop infinito e cria evidência observável de falha |
| D-014 | Paralelizar relatório analítico por categoria com processos | Aprovada pela equipe | Mantém a tarefa pesada das versões anteriores e permite comparação justa com a versão sequencial |
| D-015 | Usar Docker Compose como execução oficial | Aprovada pela equipe | Torna os nós reproduzíveis e simplifica a demonstração oral |
| D-016 | Demonstrar falha derrubando um worker durante o processamento | Aprovada pela equipe | Prova redelivery, recuperação automática, idempotência e ausência de perda |
| D-017 | Não prometer números de desempenho antes do benchmark | Aprovada e cumprida | Os números foram registrados somente após duas execuções reproduzíveis, com dados brutos, ambiente, metodologia e resultados versionados |
| D-018 | Partir exclusivamente da `main` e desenvolver em `versao-simplificada` | Aprovada e definitiva | Nenhuma branch anterior de Parte 2 foi consultada, auditada ou reutilizada; o merge só ocorrerá após validação plena |
| D-019 | Usar o nível Exceeding (4/4) dos oito critérios como definição de pronto | Aprovada pela equipe | O objetivo explícito é alcançar 4/4 em toda a rubrica |
| D-020 | Medir 1, 2, 4 e até o limite útil de processos | Aprovada pela equipe | Permite explicar onde o ganho para de crescer |
| D-021 | A recuperação do worker deve ocorrer sem intervenção manual | Aprovada pela equipe | Mensagem sem ack volta à fila e outra instância a processa automaticamente |
| D-022 | Criar documento de aprendizado e apresentação após a implementação | Aprovada e programada para o final | O documento será produzido quando código, testes e resultados reais estiverem concluídos |
| D-023 | Cada registro de IA guarda o prompt exato e as mudanças associadas | Aprovada pela equipe | Permite mostrar o que foi solicitado, corrigido e por que cada sugestão foi aceita ou rejeitada |
| D-024 | Devolução exige posse de um empréstimo ativo | Aprovada pela equipe | `loan_id`, `client_id`, estado ativo, transação e constraint impedem devolução duplicada ou estoque acima da capacidade |
| D-025 | Cliente normalmente fica inscrito, mas pode reconectar | Aprovada pela equipe | Fila de resposta durável e `client_id` estável preservam respostas durante uma desconexão temporária |
| D-026 | Expor o PostgreSQL do Compose em `localhost:15432` | Decisão operacional implementada | A porta 5432 já pertencia a um PostgreSQL nativo e 55432 estava reservada pelo Windows; internamente o contêiner continua usando a porta padrão 5432 |

## Backlog posterior à segunda entrega

Por decisão da equipe, melhorias que não são necessárias para a rubrica ficam
para depois da segunda entrega. A prioridade agora é uma implementação simples,
compreensível e fácil de estudar.

| Sugestão | Estado | Motivo |
|---|---|---|
| Adicionar um gateway HTTP/TCP entre clientes e RabbitMQ | Pós-entrega 2 | Melhora segurança e esconde AMQP, mas adiciona um serviço agora desnecessário |
| Cluster RabbitMQ com três nós | Pós-entrega 2 | Toleraria falha do broker; a rubrica será demonstrada com falha de consumidor |
| Replicação/failover automático do PostgreSQL | Pós-entrega 2 | Importante em produção, mas amplia muito o escopo acadêmico |
| Avaliar novos microsserviços | Pós-entrega 2 | Só deverá ocorrer se uma responsabilidade real justificar a separação |

## Alternativas rejeitadas para a segunda entrega

| Alternativa | Motivo |
|---|---|
| Manter pedido sem ack até surgir exemplar | Prende consumidor, dificulta recuperação e não persiste a posição da espera como estado de negócio |
| Usar somente threads para o critério de paralelismo | Threads atuais demonstram concorrência de I/O, não ganho de CPU exigido pela rubrica |
| Criar uma fila para cada livro | Multiplica recursos e configuração sem isolar falhas do broker; seis filas de categoria são suficientes |

## Diretrizes respondidas pela equipe

1. O trabalho parte exclusivamente da `main`.
2. Categorias: ficção científica, romance, aventura, ação, suspense e terror.
3. O cliente permanece inscrito normalmente, mas pode reconectar com o mesmo
   `client_id` e recuperar respostas persistidas.
4. A fila de espera é persistida no PostgreSQL.
5. O relatório analítico por categoria é a tarefa pesada do domínio.

## Questão resolvida nesta revisão

As novas imagens confirmaram o oitavo critério, **Oral Presentation and Team
Collaboration**, resolvendo a diferença entre as sete linhas iniciais e o total
de 32 pontos.

