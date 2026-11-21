"""Gateway V2 manual para o gerador de carga ``cliente_test.py``.

Requer ``DATABASE_URL`` apontando para um PostgreSQL de desenvolvimento.
O script aplica schema e seed de forma idempotente; ele não inicia worker.
"""

import logging
import os
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent))

from database.connection import aplicar_schema
from network.servidor import Servidor


if __name__ == "__main__":
    if not os.getenv("DATABASE_URL"):
        raise SystemExit("Configure DATABASE_URL antes de iniciar o Gateway V2.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    aplicar_schema(incluir_seed=True)
    servidor = Servidor()

    print("=" * 60)
    print("GATEWAY V2 DE TESTE — worker desligado")
    print("=" * 60)
    print("Rode 'python test/cliente_test.py' em outro terminal.")
    print("Ctrl+C para encerrar.")

    try:
        servidor.iniciar()
    except KeyboardInterrupt:
        servidor.encerrar()
        print("\nGateway encerrado.")
