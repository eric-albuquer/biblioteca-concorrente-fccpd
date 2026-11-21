"""Executa PEDIR/STATUS/DEVOLVER pelo Gateway publicado no host."""

from __future__ import annotations

import argparse
import json

from network.cliente import Cliente


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--usuario-id", type=int, default=1)
    parser.add_argument("--livro-id", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()

    with Cliente(host=args.host, porta=args.port) as cliente:
        aceite = cliente.pedir_livro(args.usuario_id, args.livro_id)
        print("PEDIR aceite:", json.dumps(aceite, ensure_ascii=False))
        concluido = cliente.aguardar_conclusao(
            aceite["request_id"], timeout=args.timeout
        )
        print("PEDIR final:", json.dumps(concluido, ensure_ascii=False))

        devolucao = cliente.devolver_livro(
            args.usuario_id, concluido["emprestimo_id"]
        )
        print("DEVOLVER aceite:", json.dumps(devolucao, ensure_ascii=False))
        devolvida = cliente.aguardar_conclusao(
            devolucao["request_id"], timeout=args.timeout
        )
        print("DEVOLVER final:", json.dumps(devolvida, ensure_ascii=False))


if __name__ == "__main__":
    main()

