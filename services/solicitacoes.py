"""Aceitação durável e consulta de comandos do Gateway V2."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from database.connection import transacao
from services.comum import (
    ConflitoIdempotencia,
    EntidadeNaoEncontrada,
    bloquear_request_id,
    hash_payload,
    normalizar_uuid,
    selecionar_solicitacao,
    validar_repeticao,
)


class ServicoSolicitacoes:
    """Registra solicitação e outbox atomicamente, sem aplicar o domínio."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn

    def aceitar_pedido(
        self, request_id: UUID | str, usuario_id: int, livro_id: int
    ) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        payload_normalizado = {
            "tipo": "PEDIR",
            "usuario_id": usuario_id,
            "livro_id": livro_id,
        }
        payload_hash = hash_payload(payload_normalizado)

        with transacao(self.dsn) as conexao:
            bloquear_request_id(conexao, request_uuid)
            existente = selecionar_solicitacao(conexao, request_uuid)
            if existente:
                return validar_repeticao(
                    existente, tipo="PEDIR", payload_hash=payload_hash
                )

            self._validar_usuario(conexao, usuario_id)
            livro = conexao.execute(
                """
                SELECT id, ultimo_seq_pedido
                FROM livros
                WHERE id = %s
                FOR UPDATE
                """,
                (livro_id,),
            ).fetchone()
            if not livro:
                raise EntidadeNaoEncontrada(f"Livro {livro_id} não existe")

            ordem_fila = livro["ultimo_seq_pedido"] + 1
            conexao.execute(
                """
                UPDATE livros
                SET ultimo_seq_pedido = %s
                WHERE id = %s
                """,
                (ordem_fila, livro_id),
            )
            solicitacao = conexao.execute(
                """
                INSERT INTO solicitacoes (
                    request_id, tipo, usuario_id, livro_id, ordem_fila,
                    status, payload_hash
                )
                VALUES (%s, 'PEDIR', %s, %s, %s, 'RECEBIDO', %s)
                RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                          ordem_fila, status, erro, payload_hash, criado_em,
                          atualizado_em, concluido_em
                """,
                (
                    request_uuid,
                    usuario_id,
                    livro_id,
                    ordem_fila,
                    payload_hash,
                ),
            ).fetchone()
            self._inserir_outbox(
                conexao,
                request_uuid,
                "PEDIR",
                {"request_id": str(request_uuid), **payload_normalizado},
            )
            return solicitacao

    def aceitar_devolucao(
        self, request_id: UUID | str, usuario_id: int, emprestimo_id: int
    ) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        payload_normalizado = {
            "tipo": "DEVOLVER",
            "usuario_id": usuario_id,
            "emprestimo_id": emprestimo_id,
        }
        payload_hash = hash_payload(payload_normalizado)

        with transacao(self.dsn) as conexao:
            bloquear_request_id(conexao, request_uuid)
            existente = selecionar_solicitacao(conexao, request_uuid)
            if existente:
                return validar_repeticao(
                    existente, tipo="DEVOLVER", payload_hash=payload_hash
                )

            self._validar_usuario(conexao, usuario_id)
            emprestimo = conexao.execute(
                "SELECT id, livro_id FROM emprestimos WHERE id = %s",
                (emprestimo_id,),
            ).fetchone()
            if not emprestimo:
                raise EntidadeNaoEncontrada(
                    f"Empréstimo {emprestimo_id} não existe"
                )

            solicitacao = conexao.execute(
                """
                INSERT INTO solicitacoes (
                    request_id, tipo, usuario_id, livro_id, emprestimo_id,
                    status, payload_hash
                )
                VALUES (%s, 'DEVOLVER', %s, %s, %s, 'RECEBIDO', %s)
                RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                          ordem_fila, status, erro, payload_hash, criado_em,
                          atualizado_em, concluido_em
                """,
                (
                    request_uuid,
                    usuario_id,
                    emprestimo["livro_id"],
                    emprestimo_id,
                    payload_hash,
                ),
            ).fetchone()
            self._inserir_outbox(
                conexao,
                request_uuid,
                "DEVOLVER",
                {
                    "request_id": str(request_uuid),
                    "livro_id": emprestimo["livro_id"],
                    **payload_normalizado,
                },
            )
            return solicitacao

    def consultar_status(self, request_id: UUID | str) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        with transacao(self.dsn) as conexao:
            solicitacao = selecionar_solicitacao(conexao, request_uuid)
            if not solicitacao:
                raise EntidadeNaoEncontrada(
                    f"Solicitação {request_uuid} não existe"
                )
            return solicitacao

    @staticmethod
    def _validar_usuario(conexao, usuario_id: int) -> None:
        if not conexao.execute(
            "SELECT 1 FROM usuarios WHERE id = %s", (usuario_id,)
        ).fetchone():
            raise EntidadeNaoEncontrada(f"Usuário {usuario_id} não existe")

    @staticmethod
    def _inserir_outbox(
        conexao,
        request_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        conexao.execute(
            """
            INSERT INTO outbox (request_id, event_type, payload)
            VALUES (%s, %s, %s)
            """,
            (request_id, event_type, Jsonb(payload)),
        )


__all__ = [
    "ConflitoIdempotencia",
    "EntidadeNaoEncontrada",
    "ServicoSolicitacoes",
]

