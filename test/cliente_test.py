"""Teste de rede — lado CLIENTE.

Dispara muitos clientes concorrentes contra o servidor de teste e valida o
resultado. Todos ficam parados numa barreira e só então enviam o PEDIR, para
que a disputa pelos exemplares aconteça de verdade e não em fila natural.

Uso (com o `servidor_test.py` rodando em outro terminal):
    python3 test/cliente_test.py
    python3 test/cliente_test.py 200     # número de clientes

Sai com código 1 se qualquer verificação falhar.
"""

import threading
import random
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.cliente import Cliente

NUM_CLIENTES = 100

# Precisa bater com o catálogo do servidor_test.py.
LIVROS = ["Duna", "1984", "O Hobbit"]

TITULO_INEXISTENTE = "Livro Que Nao Existe"

# Quanto tempo o cliente segura o livro antes de devolver.
TEMPO_DE_LEITURA = (0.3, 0.8)


class Resultado:
    def __init__(self, numero, titulo):
        self.numero = numero
        self.titulo = titulo
        self.espera = None      # Segundos entre pedir e ser atendido
        self.ficha = None
        self.erro = None

    @property
    def ok(self):
        return self.erro is None


def executar_cliente(resultado, barreira):
    """Ciclo completo de um cliente: conecta, pede, lê, devolve, desconecta."""
    cliente = None

    try:
        cliente = Cliente()

    except Exception as erro:
        resultado.erro = f"falha ao conectar: {erro}"
        barreira.abort()
        return

    try:
        # Todos largam juntos, para maximizar a concorrência no servidor.
        try:
            barreira.wait(timeout=30)

        except threading.BrokenBarrierError:
            pass

        inicio = time.monotonic()

        resultado.ficha = cliente.pedir_livro(resultado.titulo)

        resultado.espera = time.monotonic() - inicio

        if resultado.titulo not in resultado.ficha:
            resultado.erro = "a ficha recebida não corresponde ao título pedido"
            return

        time.sleep(random.uniform(*TEMPO_DE_LEITURA))

        resposta = cliente.devolver_livro(resultado.titulo)

        if resposta != "Livro devolvido.":
            resultado.erro = f"resposta inesperada na devolução: {resposta!r}"

    except Exception as erro:
        resultado.erro = f"{type(erro).__name__}: {erro}"

    finally:
        cliente.desconectar()


def testar_carga(quantidade):
    print(f"Disparando {quantidade} clientes concorrentes...")

    resultados = [
        Resultado(numero, random.choice(LIVROS)) for numero in range(quantidade)
    ]

    barreira = threading.Barrier(quantidade)

    threads = [
        threading.Thread(target=executar_cliente, args=(resultado, barreira))
        for resultado in resultados
    ]

    inicio = time.monotonic()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return resultados, time.monotonic() - inicio


def testar_erros():
    """Verifica os caminhos de erro do protocolo, não só o caminho feliz."""
    falhas = []

    cliente = Cliente()

    try:
        try:
            cliente.pedir_livro(TITULO_INEXISTENTE)
            falhas.append("PEDIR de título inexistente deveria gerar ERRO")

        except RuntimeError:
            pass

        try:
            cliente.devolver_livro(TITULO_INEXISTENTE)
            falhas.append("DEVOLVER de título inexistente deveria gerar ERRO")

        except RuntimeError:
            pass

        # A conexão precisa continuar utilizável depois de um ERRO.
        try:
            ficha = cliente.pedir_livro(LIVROS[0])
            cliente.devolver_livro(LIVROS[0])

            if LIVROS[0] not in ficha:
                falhas.append("ficha inválida após um ERRO na mesma conexão")

        except Exception as erro:
            falhas.append(f"conexão inutilizável após ERRO: {erro}")

    finally:
        cliente.desconectar()

    return falhas


def relatar(resultados, duracao, falhas_de_erro):
    sucessos = [r for r in resultados if r.ok]
    falhados = [r for r in resultados if not r.ok]

    print()
    print("=" * 60)
    print("RESULTADO")
    print("=" * 60)
    print(f"Clientes:  {len(resultados)}")
    print(f"Sucessos:  {len(sucessos)}")
    print(f"Falhas:    {len(falhados)}")
    print(f"Tempo:     {duracao:.2f}s")

    print()
    print("ESPERA NA FILA (por título)")
    print("-" * 60)

    for titulo in LIVROS:
        do_titulo = [r for r in sucessos if r.titulo == titulo]

        if not do_titulo:
            continue

        esperas = [r.espera for r in do_titulo]

        # Espera acima de 100ms indica que o cliente passou pela fila
        # em vez de ser atendido direto pelo contador.
        enfileirados = sum(1 for espera in esperas if espera > 0.1)

        print(
            f"{titulo:12} "
            f"atendidos: {len(do_titulo):3}  "
            f"media: {sum(esperas) / len(esperas):5.2f}s  "
            f"maxima: {max(esperas):5.2f}s  "
            f"passaram pela fila: {enfileirados:3}"
        )

    if falhados:
        print()
        print("FALHAS")
        print("-" * 60)

        for resultado in falhados[:10]:
            print(f"  cliente {resultado.numero} ({resultado.titulo}): {resultado.erro}")

        if len(falhados) > 10:
            print(f"  ... e mais {len(falhados) - 10}")

    print()
    print("VALIDAÇÃO")
    print("-" * 60)

    problemas = list(falhas_de_erro)

    if falhados:
        problemas.append(f"{len(falhados)} cliente(s) falharam")

    total_enfileirado = sum(1 for r in sucessos if r.espera and r.espera > 0.1)

    if total_enfileirado == 0:
        problemas.append(
            "nenhum cliente esperou na fila — aumente o número de clientes "
            "para que o teste de concorrência tenha valor"
        )

    if problemas:
        print("❌ TESTE FALHOU")

        for problema in problemas:
            print(f"  - {problema}")

        return 1

    print("✅ TESTE PASSOU")
    print(f"  - {len(sucessos)} clientes atendidos e devolvidos sem erro")
    print(f"  - {total_enfileirado} passaram pela fila de espera e foram liberados")
    print("  - caminhos de erro do protocolo respondem ERRO corretamente")

    return 0


if __name__ == "__main__":
    quantidade = int(sys.argv[1]) if len(sys.argv) > 1 else NUM_CLIENTES

    try:
        falhas_de_erro = testar_erros()

    except ConnectionRefusedError:
        print("Servidor não encontrado. Rode 'python3 test/servidor_test.py' antes.")
        sys.exit(1)

    resultados, duracao = testar_carga(quantidade)

    sys.exit(relatar(resultados, duracao, falhas_de_erro))
