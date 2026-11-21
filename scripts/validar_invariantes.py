"""Valida consistência persistente e termina com código não zero em violações."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import pika

from database.connection import conectar
from messaging.config import parametros_conexao
from messaging.topology import DEAD_LETTER_QUEUE, declarar_topologia


def validar_estado(
    dsn: str | None = None,
    rabbitmq_url: str | None = None,
    *,
    verificar_rabbit: bool = True,
    permitir_dlq: bool = False,
) -> dict[str, Any]:
    resultado: dict[str, Any] = {"livros": [], "violacoes": []}
    with conectar(dsn) as conexao:
        livros = conexao.execute(
            """
            SELECT l.id AS livro_id,
                   l.titulo,
                   l.capacidade,
                   l.disponiveis,
                   (
                       SELECT COUNT(*) FROM emprestimos e
                       WHERE e.livro_id = l.id AND e.devolvido_em IS NULL
                   )::int AS emprestimos_ativos,
                   (
                       SELECT COUNT(*) FROM solicitacoes s
                       WHERE s.livro_id = l.id
                         AND s.tipo = 'PEDIR'
                         AND s.status = 'AGUARDANDO'
                   )::int AS aguardando
            FROM livros l
            ORDER BY l.id
            """
        ).fetchall()
        resultado["livros"] = livros
        for livro in livros:
            livro["fila_pendente"] = conexao.execute(
                """
                SELECT request_id, usuario_id, ordem_fila, status
                FROM solicitacoes
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status IN ('RECEBIDO', 'AGUARDANDO')
                ORDER BY ordem_fila
                """,
                (livro["livro_id"],),
            ).fetchall()
            livro["pedidos_atendidos"] = conexao.execute(
                """
                SELECT request_id, usuario_id, ordem_fila, emprestimo_id
                FROM solicitacoes
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status = 'CONCLUIDO'
                ORDER BY ordem_fila
                """,
                (livro["livro_id"],),
            ).fetchall()
            if not 0 <= livro["disponiveis"] <= livro["capacidade"]:
                resultado["violacoes"].append(
                    f"{livro['titulo']}: disponíveis fora dos limites"
                )
            if (
                livro["disponiveis"] + livro["emprestimos_ativos"]
                != livro["capacidade"]
            ):
                resultado["violacoes"].append(
                    f"{livro['titulo']}: disponíveis + ativos != capacidade"
                )

        duplicados = conexao.execute(
            """
            SELECT borrow_request_id, COUNT(*) AS total
            FROM emprestimos
            GROUP BY borrow_request_id
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        resultado["emprestimos_duplicados"] = duplicados
        if duplicados:
            resultado["violacoes"].append("borrow_request_id duplicado")

        fifo = conexao.execute(
            """
            SELECT anterior.request_id AS anterior,
                   anterior.ordem_fila AS ordem_anterior,
                   posterior.request_id AS posterior,
                   posterior.ordem_fila AS ordem_posterior
            FROM solicitacoes anterior
            JOIN solicitacoes posterior
              ON posterior.livro_id = anterior.livro_id
             AND posterior.tipo = 'PEDIR'
             AND posterior.status = 'CONCLUIDO'
             AND posterior.ordem_fila > anterior.ordem_fila
            WHERE anterior.tipo = 'PEDIR'
              AND anterior.status IN ('RECEBIDO', 'AGUARDANDO')
            LIMIT 20
            """
        ).fetchall()
        resultado["violacoes_fifo"] = fifo
        if fifo:
            resultado["violacoes"].append(
                "pedido posterior concluído enquanto anterior permanece pendente"
            )

        concluidos_sem_emprestimo = conexao.execute(
            """
            SELECT s.request_id
            FROM solicitacoes s
            LEFT JOIN emprestimos e ON e.id = s.emprestimo_id
            WHERE s.tipo = 'PEDIR'
              AND s.status = 'CONCLUIDO'
              AND e.id IS NULL
            """
        ).fetchall()
        if concluidos_sem_emprestimo:
            resultado["violacoes"].append(
                "pedido concluído sem empréstimo correspondente"
            )

        devolucoes_incompletas = conexao.execute(
            """
            SELECT s.request_id
            FROM solicitacoes s
            JOIN emprestimos e ON e.id = s.emprestimo_id
            WHERE s.tipo = 'DEVOLVER'
              AND s.status = 'CONCLUIDO'
              AND e.devolvido_em IS NULL
            """
        ).fetchall()
        if devolucoes_incompletas:
            resultado["violacoes"].append(
                "devolução concluída com empréstimo ainda ativo"
            )

        resultado["outbox_pendente"] = conexao.execute(
            "SELECT COUNT(*) AS total FROM outbox WHERE publicado_em IS NULL"
        ).fetchone()["total"]

    resultado["dlq"] = None
    if verificar_rabbit:
        rabbit = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
        try:
            channel = rabbit.channel()
            declarar_topologia(channel)
            fila = channel.queue_declare(queue=DEAD_LETTER_QUEUE, passive=True)
            resultado["dlq"] = fila.method.message_count
        finally:
            rabbit.close()
        if resultado["dlq"] and not permitir_dlq:
            resultado["violacoes"].append(
                f"DLQ contém {resultado['dlq']} mensagem(ns)"
            )

    resultado["ok"] = not resultado["violacoes"]
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-rabbit", action="store_true")
    parser.add_argument("--allow-dlq", action="store_true")
    args = parser.parse_args()
    resultado = validar_estado(
        os.getenv("DATABASE_URL"),
        os.getenv("RABBITMQ_URL"),
        verificar_rabbit=not args.skip_rabbit,
        permitir_dlq=args.allow_dlq,
    )
    print(json.dumps(resultado, ensure_ascii=False, default=str, indent=2))
    raise SystemExit(0 if resultado["ok"] else 1)


if __name__ == "__main__":
    main()
