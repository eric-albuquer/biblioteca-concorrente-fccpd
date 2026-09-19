# Biblioteca Concorrente — Cliente/Servidor TCP

Protótipo de um sistema de empréstimo de livros com **servidor TCP multithread**.
Vários clientes disputam um número limitado de exemplares ao mesmo tempo: quem
chega e encontra o estoque zerado **fica bloqueado numa fila de espera FIFO** até
alguém devolver, em vez de receber um erro ou ficar consultando o servidor em
laço.

> Disciplina: FCCPD — Fundamentos de Concorrência e Computação Distribuída
> Equipe: *(Amanda Luz, Eric Albuquerque, Gabriel Aniceto, Mircio Ferreira)*

---

## 1. O que o sistema faz

Uma biblioteca tem vários **títulos**, e cada título tem um número fixo de
**exemplares**. O cliente pede um título; se houver exemplar livre, recebe na
hora a ficha do livro (título, autor, edição, gênero, ano, preço). Se não
houver, a requisição **não falha e não gira em laço** — a thread daquele cliente
dorme até que uma devolução a acorde, respeitando a ordem de chegada.

O problema é interessante porque junta as três dificuldades da disciplina:
concorrência (muitas threads no mesmo estado), exclusão mútua (o contador de
exemplares não pode ser corrompido) e comunicação em rede (mensagens sobre um
fluxo TCP).

### Mapa das partes

| Arquivo | Papel | Conhece rede? |
|---|---|---|
| `biblioteca.py` | Domínio: `Livro`, `Estoque`, `Biblioteca`. Toda a lógica de concorrência mora aqui. | Não |
| `network/protocolo.py` | Enquadramento das mensagens sobre TCP. Serializa e desserializa. | Sim |
| `network/servidor.py` | Aceita conexões, cria uma thread por cliente, traduz mensagem em chamada de domínio. | Sim |
| `network/cliente.py` | Biblioteca cliente. Esconde o socket atrás de `pedir_livro` / `devolver_livro`. | Sim |
| `test/servidor_test.py` | Sobe um servidor de teste com estoque pequeno, para forçar fila. | — |
| `test/cliente_test.py` | Dispara N clientes concorrentes e valida o resultado. | — |
| `ARQUITETURA.md` | Diagramas Mermaid: componentes e fluxo de dados. | — |

A separação central é esta: **`biblioteca.py` não importa `socket`**. O domínio é
testável e executável sem rede nenhuma, e a camada `network/` é só um adaptador.
Isso significa que os bugs de concorrência podem ser investigados sem subir
servidor.

### Diagrama

O mapa completo — componentes, threads e fluxo de dados passo a passo — está em
**[`ARQUITETURA.md`](ARQUITETURA.md)**, em dois diagramas Mermaid.

### Por que cada ferramenta

| Escolha | Por quê | Alternativa descartada |
|---|---|---|
| **Python + `threading`** | O gargalo aqui é espera (I/O de rede e espera por exemplar), não CPU. O GIL não atrapalha: uma thread bloqueada em `recv()` ou `Event.wait()` **libera o GIL**, então as outras rodam. | `multiprocessing`: o estado (`Estoque`) é compartilhado e mutável. Com processos, ele precisaria de memória compartilhada ou IPC, muito mais complexo sem ganho. |
| **Thread por cliente** | Cada cliente pode ficar arbitrariamente bloqueado na fila de espera. Com uma thread dedicada, esse bloqueio é expresso como uma chamada simples e sequencial (`evento.wait()`), sem máquina de estados. | `select`/`asyncio`: exigiria reescrever a espera como callback/corrotina e espalhar o estado do cliente pelo código. |
| **`threading.Lock`** | A seção crítica é curtíssima (comparar e decrementar um inteiro, mexer num deque). Lock simples é o mais barato e o mais fácil de auditar. | `RLock`: não há recursão. `Semaphore`: ver a seção 2. |
| **`threading.Event` por cliente** | Precisamos acordar **um** cliente específico — o primeiro da fila. `Event` é a primitiva exata para isso. | `Condition.notify()`: acorda uma thread arbitrária, sem garantia de FIFO. |
| **`collections.deque`** | Fila FIFO com `append`/`popleft` em O(1). | `list.pop(0)`: O(n). |
| **TCP** | Empréstimo e devolução não podem ser perdidos nem reordenados. TCP dá entrega confiável e ordenada de graça. | UDP: exigiria reimplementar confirmação, retransmissão e ordenação. |
| **`struct` + cabeçalho binário** | Enquadramento explícito e de tamanho fixo (5 bytes). Ver a seção 3. | JSON/pickle: ver a seção 3. |
| **Só biblioteca padrão** | Zero dependências: `python3 network/servidor.py` roda em qualquer máquina com Python 3.10+. | Frameworks de rede seriam peso morto para 4 operações. |

---

## 2. Concorrência: onde acontece e como está protegida

### Onde há execução simultânea

| # | Ponto | Threads envolvidas | Estado compartilhado | Proteção |
|---|---|---|---|---|
| 1 | Laço `accept()` | 1 (principal) | o socket de escuta | única thread, não precisa |
| 2 | `atender_cliente` | **1 por cliente conectado** | nenhum entre si — cada uma tem seu socket | isolamento natural |
| 3 | `Estoque.retirar_exemplar` | todas as threads de cliente | `disponiveis`, `fila_espera` | `estoque.lock` |
| 4 | `Estoque.devolver_exemplar` | todas as threads de cliente | `disponiveis`, `fila_espera` | `estoque.lock` |
| 5 | `Biblioteca.mostrar_estoques` | thread monitor | lê `disponiveis` e `fila_espera` | `estoque.lock` na leitura |
| 6 | `Biblioteca.cadastrar_livro` | quem cadastrar títulos | o dict `estoques` | `biblioteca.lock` |

São **dois níveis de lock**, cada um com um dono claro:

- `biblioteca.lock` protege a **estrutura** do catálogo — impede que dois
  cadastros simultâneos do mesmo título criem dois `Estoque` e que um deles suma.
- `estoque.lock` protege o **conteúdo** de um título — o contador e a fila.

O ponto 5 merece atenção: o monitor lê `disponiveis` e `len(fila_espera)` dentro
do `with estoque.lock` e só depois imprime. Se lesse fora, poderia exibir um
estado que nunca existiu (contador de um instante, fila de outro).

### A condição de corrida concreta

Sem lock, dois clientes podem executar isto intercalado com `disponiveis == 1`:

```
Thread A: if self.disponiveis:        -> 1, verdadeiro
Thread B: if self.disponiveis:        -> 1, verdadeiro     <- ainda não decrementou
Thread A: self.disponiveis -= 1       -> 0
Thread B: self.disponiveis -= 1       -> -1                <- exemplar fantasma
```

Os dois recebem o livro e o contador fica negativo. É uma corrida
*check-then-act* clássica: o teste e a ação precisam ser **uma operação
atômica**. Por isso o `if` e o `-=` estão dentro do mesmo `with self.lock`.

### Granularidade: um lock por título

O lock do contador é atributo de `Estoque`, não de `Biblioteca`. Quem pede
"Duna" não bloqueia quem pede "1984" — as disputas são independentes e rodam em
paralelo. Um lock global seria correto, mas serializaria a biblioteca inteira: o
`biblioteca.lock` existe só para o cadastro, que acontece na inicialização e não
no caminho crítico.

### Como o deadlock é evitado

Três propriedades garantem isso:

1. **Nenhum caminho de código segura dois locks ao mesmo tempo.**
   `cadastrar_livro` segura só o `biblioteca.lock`; `retirar_exemplar` e
   `devolver_exemplar` seguram só o `estoque.lock` do seu título. Sem
   aninhamento não há como formar espera circular — a condição de Coffman
   indispensável para deadlock nunca se forma.
2. **`evento.wait()` fica FORA do `with self.lock`.** Este é o ponto mais
   importante do projeto. Se a espera acontecesse com o lock na mão, a thread
   dormiria segurando o lock, nenhuma devolução conseguiria entrar em
   `devolver_exemplar()` para acordá-la, e a biblioteca inteira travaria. O
   `return` dentro do `with` e o `wait()` depois dele são deliberados:

   ```python
   def retirar_exemplar(self):
       with self.lock:
           if self.disponiveis:
               self.disponiveis -= 1
               return              # sai com o lock, caminho rápido
           pedido = threading.Event()
           self.fila_espera.append(pedido)
       # lock já liberado aqui
       pedido.wait()
   ```
3. **A seção crítica não faz I/O.** Nada de `send`, `recv` ou `print` com o lock
   na mão. Só aritmética e operações de deque, todas de duração limitada.

### Como a inanição (*starvation*) é evitada

A fila é **FIFO estrita**: `append()` no fim, `popleft()` na frente. Cada
devolução acorda exatamente o cliente que está esperando há mais tempo. Não há
disputa pelo exemplar liberado — ele é **entregue** a um destinatário definido,
não colocado de volta num pote pelo qual todos brigam.

Esse é o motivo de `devolver_exemplar` usar `if/else` em vez de sempre
incrementar:

```python
if self.fila_espera:
    self.fila_espera.popleft().set()   # passa direto para quem espera
else:
    self.disponiveis += 1              # só então volta ao contador
```

Se o contador fosse incrementado **e** a fila notificada, o exemplar seria
contado duas vezes. Se fosse incrementado e ninguém notificado, quem está na
fila dormiria para sempre.

### Por que não um `Semaphore`

`threading.Semaphore` resolveria a contagem em uma linha, e é uma pergunta justa
na apresentação. Não foi usado porque o `Semaphore` do Python **não garante
ordem de liberação** — um cliente pode ficar para trás indefinidamente enquanto
recém-chegados passam na frente. A implementação com contador + `deque` de
`Event` custa poucas linhas a mais e entrega FIFO demonstrável, que é
justamente o que o teste de fila comprova.

---

## 3. Comunicação: protocolo da aplicação

### O problema que o protocolo resolve

TCP é um **fluxo de bytes**, não de mensagens. Ele não preserva fronteiras: dois
`sendall()` podem chegar num único `recv()`, e um `sendall()` pode chegar
picado. Quem só faz `sock.recv(1024)` e assume que recebeu uma mensagem inteira
tem um bug que só aparece sob carga. O protocolo existe para reconstruir essas
fronteiras.

### Formato

Cabeçalho binário de tamanho fixo, seguido do payload:

```
 0        1                    5                     5 + tamanho
 +--------+--------------------+--------------------------+
 | opera- |      tamanho       |     payload UTF-8        |
 | ção 1B |    4B big-endian   |     (tamanho bytes)      |
 +--------+--------------------+--------------------------+
```

`struct.Struct("!BI")` — `!` é big-endian sem alinhamento, `B` é um byte sem
sinal, `I` é um inteiro de 4 bytes sem sinal. Cabeçalho de **5 bytes**.

### Operações

| Código | Nome | Sentido | Payload |
|---|---|---|---|
| 1 | `PEDIR` | cliente → servidor | título desejado |
| 2 | `DEVOLVER` | cliente → servidor | título devolvido |
| 3 | `RESPOSTA` | servidor → cliente | ficha do livro, ou `"Livro devolvido."` |
| 4 | `ERRO` | servidor → cliente | mensagem de erro |

O ciclo é estritamente **requisição/resposta**: para cada mensagem enviada, o
cliente bloqueia em `receber_mensagem()` até a resposta chegar. Isso mantém o
cliente trivial e faz a espera na fila do servidor aparecer naturalmente como
latência da chamada.

### Justificativa das escolhas

**Por que prefixo de tamanho, e não um delimitador (`\n`).** Com delimitador,
seria preciso escapar o caractere sempre que aparecesse no conteúdo — e a ficha
do livro é multilinha. Com prefixo, o conteúdo é totalmente opaco: nenhum byte
é especial. E o leitor sabe de antemão quantos bytes alocar.

**Por que big-endian (`!`).** É a ordem de bytes da rede. Fixá-la garante que
cliente e servidor se entendam mesmo em arquiteturas diferentes. O `!` também
desliga o alinhamento do `struct`, então o cabeçalho tem exatamente 5 bytes em
qualquer plataforma — sem ele, o C daria 8 por padding.

**Por que binário, e não JSON.** O cabeçalho é de tamanho fixo e conhecido, o
que permite lê-lo com uma única chamada de tamanho determinado. Com JSON ainda
seria necessário resolver o enquadramento antes de conseguir parsear — o
problema não desapareceria, só mudaria de lugar.

**Por que UTF-8, com o tamanho contado em bytes.** O catálogo tem acentos
("Ficção Científica"). O campo `tamanho` conta **bytes codificados**, não
caracteres — `len(payload.encode("utf-8"))`. Contar caracteres truncaria toda
mensagem acentuada.

**Por que `receber_exatamente` tem um laço.** Um `recv(n)` pode devolver menos
de `n` bytes. O laço acumula até completar, e é o que torna o protocolo correto
para payloads grandes:

```python
while len(dados) < tamanho:
    bloco = sock.recv(tamanho - len(dados))
    if not bloco:
        raise ConnectionError("Conexão encerrada pelo cliente.")
    dados.extend(bloco)
```

**Tratamento de desconexão.** `recv` devolvendo vazio significa que o par
fechou. Virar `ConnectionError` faz a thread do cliente encerrar pelo
`except ConnectionError` em `atender_cliente`, que fecha o socket no `finally`.
Um cliente que cai não derruba o servidor nem vaza descritor.

---

## 4. Como executar

Requer **Python 3.10+** (a anotação `str | None` usa sintaxe do 3.10). Sem
dependências externas.

### Servidor

```bash
python3 network/servidor.py
```

Escuta em `0.0.0.0:9999` e mostra um painel atualizado a cada segundo com
disponíveis e tamanho da fila por título.

### Cliente único

```bash
python3 network/cliente.py
```

### Teste de rede sob carga

Dois terminais:

```bash
# terminal 1 — servidor com estoque pequeno (9 exemplares em 3 títulos)
python3 test/servidor_test.py

# terminal 2 — 100 clientes concorrentes (ou o número que passar)
python3 test/cliente_test.py
python3 test/cliente_test.py 200
```

O `cliente_test.py` põe todos os clientes numa `threading.Barrier`, para que
disparem o `PEDIR` no mesmo instante e a disputa seja real. Ao final ele
verifica que todos foram atendidos e devolveram, que a ficha recebida
corresponde ao título pedido, que os caminhos de erro respondem `ERRO`, e que
**houve de fato gente passando pela fila** — um teste de concorrência em que
ninguém espera não prova nada. Sai com código 1 se qualquer verificação falhar.

Assista ao painel do terminal 1 durante a execução: "Fila" sobe para dezenas e
depois drena até zero, e "Disponíveis" volta à capacidade original.

### Evidências coletadas

Execução do domínio com 2000 threads disputando 9 exemplares em 3 títulos, com
uma thread vigia amostrando o contador durante toda a corrida:

```
2000 threads em 0.67s  (2995 ops/s)
contador fora do intervalo valido: nenhuma ocorrencia
  Duna       disponiveis=3/3  fila=0
  1984       disponiveis=2/2  fila=0
  O Hobbit   disponiveis=4/4  fila=0
nenhuma thread travada: True
```

Nenhuma amostra fora de `0 <= disponiveis <= capacidade`, todos os estoques de
volta à capacidade original, fila vazia e nenhuma thread presa — ou seja, sem
corrida, sem deadlock e sem inanição naquela execução.

Cadastro concorrente, validando o `biblioteca.lock`: 20 threads tentando
cadastrar o mesmo título ao mesmo tempo, largando juntas de uma `Barrier`.

```
cadastro concorrente OK (1 vencedor em 20 threads, 1 estoque criado)
```

Verificações de protocolo e de servidor (via `socketpair`):

```
protocolo: enquadramento + UTF-8 OK  (header = 5 bytes)
protocolo: payload de 300KB remontado OK (exercita receber_exatamente)
servidor: B bloqueado na fila enquanto A segura o exemplar
servidor: devolução liberou B da fila
servidor: PEDIR/DEVOLVER inexistente e operação 99 respondem ERRO
```

> **Pendência de verificação:** o `servidor_test.py` + `cliente_test.py` sobre
> TCP real ainda **não foi executado** — o ambiente onde o código foi preparado
> bloqueia `bind()` de socket, inclusive em loopback. As evidências acima vêm do
> domínio puro e de `socketpair`, que exercitam a mesma lógica de concorrência e
> de protocolo, mas não o caminho `accept()`. **Rode os dois terminais antes da
> entrega e cole a saída aqui.**

---

## 5. Diário de uso de IA

Ferramenta usada: **Claude (Claude Code)**.

> **A preencher pela equipe.** A tabela abaixo registra as interações desta
> sessão. Acrescentem as de vocês — a rubrica pede o que foi pedido, o que foi
> corrigido e a demonstração de que o código é compreendido.

| # | O que foi pedido | O que a IA produziu | O que foi aceito / corrigido |
|---|---|---|---|
| 1 | Diagramas Mermaid de arquitetura e de fluxo de dados | Dois diagramas + um de estados | Aceitos os dois primeiros. **Rejeitado** o diagrama de estados (redundante) e removido. Pedimos fundo preto. |
| 2 | Corrigir erro de parse no Mermaid | Diagnóstico: `;` dentro de um `Note` é separador de statement no Mermaid | Aceito. Também trocados os `<br/>` em rótulos de state diagram, que não renderizam. |
| 3 | Reescrever os testes após o refactor | Suíte `unittest` extensa + alterações em `servidor.py` para testabilidade | **Rejeitado.** Escopo grande demais e mexia no `servidor.py` além do necessário. Revertido. |
| 4 | Atualizar diagramas após o refactor | Diagramas atualizados + seção "Pontos de atenção" | Aceito. A IA apontou um bug que não tínhamos visto. |
| 5 | Melhorar nomes, README, dividir o teste de rede | Renomeações, `servidor_test.py`, `cliente_test.py`, este README | *(revisar e anotar o que a equipe ajustou)* |

### O que a equipe precisa saber explicar na arguição

Se a banca perguntar "por que aqui e não ali", estes são os pontos:

- **Por que `evento.wait()` está fora do `with self.lock`** (seção 2) — é a
  diferença entre funcionar e travar tudo.
- **Por que `devolver_exemplar` não incrementa quando há fila** — senão o
  exemplar é contado duas vezes.
- **Por que o lock é por `Estoque` e não por `Biblioteca`** — títulos diferentes
  não competem entre si.
- **Por que `receber_exatamente` precisa de um laço** — TCP é fluxo, não
  mensagem.
- **Por que o tamanho no cabeçalho conta bytes e não caracteres** — UTF-8 com
  acentos.
- **Por que não usamos `Semaphore`** — não garante FIFO.

---

## 6. Roteiro da apresentação (10 minutos)

Sugestão de divisão para que **todos falem**, como a rubrica exige.

| Tempo | Tema | Quem |
|---|---|---|
| 0–1,5 min | Problema e visão geral. Por que empréstimo de livros é um bom caso de concorrência. | *(nome)* |
| 1,5–3,5 min | Arquitetura: diagrama de componentes, separação domínio/rede, por que thread por cliente. | *(nome)* |
| 3,5–6 min | **Concorrência** (a parte que mais pesa): a corrida no contador, o lock, o `wait()` fora do lock, FIFO contra inanição. | *(nome)* |
| 6–7,5 min | Protocolo: enquadramento, cabeçalho `!BI`, por que não JSON nem `pickle`. | *(nome)* |
| 7,5–9,5 min | **Demo ao vivo**: servidor num terminal, 200 clientes no outro, painel mostrando a fila subir e drenar. | *(nome)* |
| 9,5–10 min | Uso de IA, bug encontrado, limitações conhecidas. | *(nome)* |

**Antes de apresentar:**

- Ensaiar a demo com o servidor já rodando — subir na hora custa tempo.
- Ter os diagramas do `ARQUITETURA.md` renderizados (o VS Code com a extensão
  Mermaid, ou colados como imagem), não em código-fonte no slide.
- Deixar aberto o trecho do `retirar_exemplar` — é o slide mais provável de
  gerar pergunta.
- Ler a seção "Pontos de atenção" do `ARQUITETURA.md`: são as limitações
  conhecidas, e admiti-las antes de a banca apontar conta a favor.

---

## 7. Limitações conhecidas

- A devolução **não confere posse** — é feita só pelo título, então um cliente
  pode devolver algo que nunca pegou e inflar o estoque. É a limitação
  registrada em [`ARQUITETURA.md`](ARQUITETURA.md).
- Não há tempo limite na fila de espera: um cliente pode esperar
  indefinidamente se ninguém devolver.