"""Carga manual do contrato V2 com aceitação e consulta de status.

O worker permanece desligado: todos os comandos válidos devem ficar em
``RECEBIDO`` e ser recuperáveis por UUID.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import random
import sys
import time
from uuid import uuid4


sys.path.append(str(Path(__file__).resolve().parent.parent))

from network.cliente import Cliente


NUM_CLIENTES = 100


def executar_cliente(numero: int) -> dict:
    request_id = uuid4()
    usuario_id = (numero % 4) + 1
    livro_id = random.randint(1, 3)
    inicio = time.monotonic()

    with Cliente() as cliente:
        aceite = cliente.pedir_livro(usuario_id, livro_id, request_id)
        status = cliente.consultar_status(request_id)

    return {
        "request_id": str(request_id),
        "aceite": aceite,
        "status": status,
        "latencia_ms": (time.monotonic() - inicio) * 1000,
    }


def testar_carga(quantidade: int) -> tuple[list[dict], list[str], float]:
    resultados: list[dict] = []
    erros: list[str] = []
    inicio = time.monotonic()

    with ThreadPoolExecutor(max_workers=min(quantidade, 100)) as executor:
        futuros = [executor.submit(executar_cliente, numero) for numero in range(quantidade)]
        for futuro in as_completed(futuros):
            try:
                resultados.append(futuro.result())
            except Exception as erro:
                erros.append(f"{type(erro).__name__}: {erro}")

    return resultados, erros, time.monotonic() - inicio


def validar(resultados: list[dict], erros: list[str]) -> list[str]:
    problemas = list(erros)
    request_ids = [resultado["request_id"] for resultado in resultados]
    if len(request_ids) != len(set(request_ids)):
        problemas.append("request_id duplicado no gerador")

    for resultado in resultados:
        aceite = resultado["aceite"]
        status = resultado["status"]
        if aceite["request_id"] != resultado["request_id"]:
            problemas.append("Gateway devolveu request_id diferente")
        if aceite["status"] != "RECEBIDO" or status["status"] != "RECEBIDO":
            problemas.append(
                f"{resultado['request_id']} não permaneceu RECEBIDO sem worker"
            )
    return problemas


if __name__ == "__main__":
    quantidade = int(sys.argv[1]) if len(sys.argv) > 1 else NUM_CLIENTES
    resultados, erros, duracao = testar_carga(quantidade)
    problemas = validar(resultados, erros)

    print(f"Solicitações: {quantidade}")
    print(f"Aceitas e consultadas: {len(resultados)}")
    print(f"Erros: {len(erros)}")
    print(f"Duração: {duracao:.2f}s")
    if resultados:
        latencias = [resultado["latencia_ms"] for resultado in resultados]
        print(f"Latência média aceite+status: {sum(latencias) / len(latencias):.2f}ms")

    if problemas:
        print("❌ TESTE V2 FALHOU")
        for problema in problemas[:20]:
            print(f"  - {problema}")
        raise SystemExit(1)

    print("✅ TESTE V2 PASSOU")
    print("  - todos os pedidos persistiram em RECEBIDO com worker desligado")
    print("  - todos os request_ids foram recuperados por STATUS")
