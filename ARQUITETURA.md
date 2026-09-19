# Arquitetura

Biblioteca concorrente com servidor TCP multithread. O domínio (`biblioteca.py`)
não conhece rede; a camada de rede (`network/`) apenas traduz bytes em chamadas
de domínio.

O controle de concorrência é feito por **contador + fila de eventos**: cada
`Estoque` guarda quantos exemplares estão livres (`disponiveis: int`) e uma fila
FIFO de `threading.Event`, um por cliente bloqueado. Não existe objeto por
exemplar — o que circula pela rede é a ficha do livro (`str(Livro)`).

## Diagrama de componentes

```mermaid
graph TB
    subgraph clientes["Clientes"]
        CLI["cliente.py<br/><b>Cliente</b><br/>pedir_livro() / devolver_livro() / desconectar()"]
    end

    subgraph proto["Protocolo — network/protocolo.py"]
        P["HEADER = struct '!BI'<br/>operacao (1B) | tamanho (4B)<br/>+ payload UTF-8<br/><br/>PEDIR | DEVOLVER | RESPOSTA | ERRO<br/><br/>enviar_mensagem() / receber_mensagem()<br/>receber_exatamente()"]
    end

    subgraph servidor["Servidor — network/servidor.py"]
        SOCK["socket TCP<br/>0.0.0.0:9999"]
        ACCEPT["Thread principal<br/>loop accept()"]
        HANDLER["Thread por cliente (daemon)<br/>atender_cliente()<br/>tratar_pedido() / tratar_devolucao()"]
        MON["Thread monitor (daemon)<br/>monitorar_estoques()<br/>clear + mostrar_estoques() a cada 1s"]
    end

    subgraph dominio["Domínio — biblioteca.py"]
        BIB["<b>Biblioteca</b><br/>estoques: dict titulo -> Estoque<br/>lock: threading.Lock (protege o cadastro)<br/><br/>cadastrar_livro()<br/>emprestar_livro() -> str | None<br/>devolver_livro() -> bool<br/>mostrar_estoques()"]
        EST["<b>Estoque</b><br/>livro: Livro<br/>disponiveis: int<br/>fila_espera: deque[Event]<br/>lock: threading.Lock<br/><br/>retirar_exemplar() / devolver_exemplar()"]
        LIV["<b>Livro</b><br/>titulo, autor, edicao,<br/>genero, ano_publicacao, preco<br/><br/>__str__() gera a ficha enviada ao cliente"]
    end

    CLI -->|"TCP"| SOCK
    CLI -.->|"usa"| P
    HANDLER -.->|"usa"| P
    SOCK --> ACCEPT
    ACCEPT -->|"spawn por conexão"| HANDLER
    ACCEPT -->|"spawn 1x no iniciar()"| MON
    HANDLER -->|"emprestar_livro(titulo)<br/>devolver_livro(titulo)"| BIB
    MON -->|"mostrar_estoques()"| BIB
    BIB -->|"1 por título"| EST
    EST -->|"metadados (1 instância)"| LIV
    EST -->|"1 Event por cliente bloqueado"| EVT["deque de threading.Event<br/>ordem FIFO"]

    classDef net fill:#000000,stroke:#7aa2f7,color:#e6e6e6,stroke-width:1.5px
    classDef dom fill:#000000,stroke:#7ec699,color:#e6e6e6,stroke-width:1.5px
    class CLI,P,SOCK,ACCEPT,HANDLER,MON net
    class BIB,EST,LIV,EVT dom

    style clientes fill:#111111,stroke:#7aa2f7,color:#e6e6e6
    style proto fill:#111111,stroke:#7aa2f7,color:#e6e6e6
    style servidor fill:#111111,stroke:#7aa2f7,color:#e6e6e6
    style dominio fill:#111111,stroke:#7ec699,color:#e6e6e6
```

## Fluxo de dados

Pedido atendido na hora, pedido que cai na fila de espera, e a devolução que
desbloqueia quem estava esperando.

```mermaid
%%{init: {"theme": "base", "themeVariables": {
  "background": "#000000",
  "primaryColor": "#000000",
  "primaryTextColor": "#e6e6e6",
  "primaryBorderColor": "#7aa2f7",
  "lineColor": "#7aa2f7",
  "actorBkg": "#000000",
  "actorBorder": "#7aa2f7",
  "actorTextColor": "#e6e6e6",
  "actorLineColor": "#555555",
  "signalColor": "#e6e6e6",
  "signalTextColor": "#e6e6e6",
  "noteBkgColor": "#000000",
  "noteBorderColor": "#7ec699",
  "noteTextColor": "#e6e6e6",
  "activationBkgColor": "#222222",
  "activationBorderColor": "#7ec699",
  "labelBoxBkgColor": "#000000",
  "labelBoxBorderColor": "#7aa2f7",
  "labelTextColor": "#e6e6e6",
  "sequenceNumberColor": "#000000"
}}}%%
sequenceDiagram
    autonumber
    participant A as Cliente A
    participant B as Cliente B
    participant S as Servidor<br/>thread por cliente
    participant BI as Biblioteca
    participant E as Estoque<br/>lock + contador + fila

    Note over A,E: 1. Pedido com exemplar disponível

    A->>S: enviar_mensagem(PEDIR, "Duna") — header !BI + payload
    S->>BI: emprestar_livro("Duna")
    BI->>E: retirar_exemplar()
    E->>E: with lock — disponiveis > 0, então disponiveis -= 1
    E-->>BI: retorna na hora
    BI-->>S: str(estoque.livro) — ficha completa
    S-->>A: enviar_mensagem(RESPOSTA, ficha)

    Note over A,E: 2. Contador zerado — Cliente B entra na fila FIFO

    B->>S: enviar_mensagem(PEDIR, "Duna")
    S->>BI: emprestar_livro("Duna")
    BI->>E: retirar_exemplar()
    activate E
    E->>E: with lock — disponiveis == 0, cria Event e faz fila_espera.append()
    E->>E: evento.wait() fora do lock — thread de B bloqueada
    Note right of E: só a thread de B para aqui. O servidor segue aceitando outros clientes

    Note over A,E: 3. Devolução de A libera quem está na frente da fila

    A->>S: enviar_mensagem(DEVOLVER, "Duna")
    S->>BI: devolver_livro("Duna")
    BI->>E: devolver_exemplar()
    E->>E: with lock — fila não vazia, então popleft().set() e disponiveis fica igual
    E-->>BI: retorna
    BI-->>S: True
    S-->>A: enviar_mensagem(RESPOSTA, "Livro devolvido.")

    E-->>BI: evento.wait() retorna, B foi atendido
    deactivate E
    BI-->>S: str(estoque.livro)
    S-->>B: enviar_mensagem(RESPOSTA, ficha)

    Note over A,E: Erro — título inexistente vira enviar_mensagem(ERRO, msg) e RuntimeError no Cliente
```

## Ponto de atenção

- **`devolver_livro` não confere posse.** A devolução é feita só pelo título
  (`Servidor.tratar_devolucao`), sem registro de quem pegou o quê. Um cliente
  pode devolver um título que nunca pediu e inflar `disponiveis` acima da
  capacidade — não há teto em `Estoque.devolver_exemplar()`.