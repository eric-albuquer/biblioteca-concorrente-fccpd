"""Teste de rede — lado SERVIDOR.

Sobe um servidor com um catálogo propositalmente pequeno, para que o número de
clientes do `cliente_test.py` seja muito maior que o de exemplares e a fila de
espera seja realmente exercitada.

Uso (terminal 1):
    python3 test/servidor_test.py

Depois, em outro terminal, rode o `cliente_test.py`.
O painel na tela mostra "Disponíveis" e "Fila" atualizando a cada segundo.
"""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.servidor import Servidor

# Capacidades pequenas de propósito: com 100 clientes disputando 9 exemplares,
# a maioria passa pela fila de espera em vez de ser atendida de imediato.
CATALOGO = [
    # titulo,     autor,      edicao, genero,              ano,  preco, capacidade
    ("Duna",      "Mircio",   1,      "Ficção Científica", 1965, 59.90, 3),
    ("1984",      "Amanda",   2,      "Distopia",          1949, 45.00, 2),
    ("O Hobbit",  "Tolkien",  4,      "Fantasia",          1937, 39.90, 4),
]


if __name__ == "__main__":
    servidor = Servidor()

    for titulo, autor, edicao, genero, ano, preco, capacidade in CATALOGO:
        servidor.biblioteca.cadastrar_livro(
            titulo, autor, edicao, genero, ano, preco, capacidade
        )

    total = sum(linha[-1] for linha in CATALOGO)

    print("=" * 60)
    print("SERVIDOR DE TESTE")
    print("=" * 60)
    print(f"Títulos:    {len(CATALOGO)}")
    print(f"Exemplares: {total}")
    print()
    print("Rode 'python3 test/cliente_test.py' em outro terminal.")
    print("Ctrl+C para encerrar.")
    print()

    try:
        servidor.iniciar()

    except KeyboardInterrupt:
        print("\nServidor encerrado.")
