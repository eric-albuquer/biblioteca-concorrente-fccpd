"""Empréstimos persistentes, idempotentes e FIFO por livro."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID

from database.connection import transacao


class RegraDeNegocioError(RuntimeError):
    """Erro esperado que não deve ser retentado como falha de infraestrutura."""


class EntidadeNaoEncontrada(RegraDeNegocioError):
    pass


class ConflitoIdempotencia(RegraDeNegocioError):
    pass


def _normalizar_uuid(request_id: UUID | str) -> UUID:
    try:
        return request_id if isinstance(request_id, UUID) else UUID(str(request_id))
    except (TypeError, ValueError, AttributeError) as erro:
        raise ValueError("request_id deve ser um UUID válido") from erro


def _hash_payload(payload: dict[str, Any]) -> str:
    canonico = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonico).hexdigest()


def _selecionar_solicitacao(conexao, request_id: UUID) -> dict[str, Any] | None:
    return conexao.execute(
        """
        SELECT request_id, tipo, usuario_id, livro_id, emprestimo_id,
               ordem_fila, status, erro, payload_hash, criado_em,
               atualizado_em, concluido_em
        FROM solicitacoes
        WHERE request_id = %s
        """,
        (request_id,),
    ).fetchone()


def _validar_repeticao(
    existente: dict[str, Any], *, tipo: str, payload_hash: str
) -> dict[str, Any]:
    if existente["tipo"] != tipo or existente["payload_hash"].strip() != payload_hash:
        raise ConflitoIdempotencia(
            "request_id já utilizado com tipo ou payload diferente"
        )
    return existente


class ServicoEmprestimos:
    """Coordena transações sem manter estoque ou fila na memória do processo."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn

    def registrar_pedido(
        self, request_id: UUID | str, usuario_id: int, livro_id: int
    ) -> dict[str, Any]:
        request_uuid = _normalizar_uuid(request_id)
        payload_hash = _hash_payload(
            {"tipo": "PEDIR", "usuario_id": usuario_id, "livro_id": livro_id}
        )

        with transacao(self.dsn) as conexao:
            # Serializa somente retransmissões do mesmo UUID, inclusive se elas
            # apontarem por engano para livros diferentes.
            conexao.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (str(request_uuid),),
            )
            existente = _selecionar_solicitacao(conexao, request_uuid)
            if existente:
                return _validar_repeticao(
                    existente, tipo="PEDIR", payload_hash=payload_hash
                )

            if not conexao.execute(
                "SELECT 1 FROM usuarios WHERE id = %s", (usuario_id,)
            ).fetchone():
                raise EntidadeNaoEncontrada(f"Usuário {usuario_id} não existe")

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
            return conexao.execute(
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

    def processar_pedidos(self, livro_id: int) -> list[dict[str, Any]]:
        """Atende os pedidos elegíveis mais antigos, não o evento disparador."""
        atendidos: list[dict[str, Any]] = []
        with transacao(self.dsn) as conexao:
            livro = conexao.execute(
                """
                SELECT id, capacidade, disponiveis
                FROM livros
                WHERE id = %s
                FOR UPDATE
                """,
                (livro_id,),
            ).fetchone()
            if not livro:
                raise EntidadeNaoEncontrada(f"Livro {livro_id} não existe")

            pendentes = conexao.execute(
                """
                SELECT request_id, usuario_id, ordem_fila
                FROM solicitacoes
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status IN ('RECEBIDO', 'AGUARDANDO')
                ORDER BY ordem_fila
                FOR UPDATE
                """,
                (livro_id,),
            ).fetchall()

            disponiveis = livro["disponiveis"]
            for pedido in pendentes:
                if disponiveis == 0:
                    break

                emprestimo = conexao.execute(
                    """
                    INSERT INTO emprestimos (
                        livro_id, usuario_id, borrow_request_id
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (borrow_request_id) DO NOTHING
                    RETURNING id, livro_id, usuario_id, borrow_request_id,
                              emprestado_em
                    """,
                    (livro_id, pedido["usuario_id"], pedido["request_id"]),
                ).fetchone()

                if emprestimo is None:
                    emprestimo = conexao.execute(
                        """
                        SELECT id, livro_id, usuario_id, borrow_request_id,
                               emprestado_em
                        FROM emprestimos
                        WHERE borrow_request_id = %s
                        """,
                        (pedido["request_id"],),
                    ).fetchone()
                else:
                    disponiveis -= 1

                conexao.execute(
                    """
                    UPDATE solicitacoes
                    SET status = 'CONCLUIDO', emprestimo_id = %s,
                        erro = NULL, concluido_em = CURRENT_TIMESTAMP
                    WHERE request_id = %s
                    """,
                    (emprestimo["id"], pedido["request_id"]),
                )
                atendidos.append(emprestimo)

            conexao.execute(
                "UPDATE livros SET disponiveis = %s WHERE id = %s",
                (disponiveis, livro_id),
            )
            conexao.execute(
                """
                UPDATE solicitacoes
                SET status = 'AGUARDANDO'
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status = 'RECEBIDO'
                """,
                (livro_id,),
            )

        return atendidos

    def registrar_e_processar_pedido(
        self, request_id: UUID | str, usuario_id: int, livro_id: int
    ) -> dict[str, Any]:
        solicitacao = self.registrar_pedido(request_id, usuario_id, livro_id)
        self.processar_pedidos(livro_id)
        with transacao(self.dsn) as conexao:
            return _selecionar_solicitacao(
                conexao, _normalizar_uuid(solicitacao["request_id"])
            )

    def devolver_emprestimo(
        self, request_id: UUID | str, usuario_id: int, emprestimo_id: int
    ) -> dict[str, Any]:
        request_uuid = _normalizar_uuid(request_id)
        payload_hash = _hash_payload(
            {
                "tipo": "DEVOLVER",
                "usuario_id": usuario_id,
                "emprestimo_id": emprestimo_id,
            }
        )

        with transacao(self.dsn) as conexao:
            conexao.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (str(request_uuid),),
            )
            existente = _selecionar_solicitacao(conexao, request_uuid)
            if existente:
                return _validar_repeticao(
                    existente, tipo="DEVOLVER", payload_hash=payload_hash
                )

            referencia = conexao.execute(
                "SELECT livro_id FROM emprestimos WHERE id = %s",
                (emprestimo_id,),
            ).fetchone()
            if not referencia:
                raise EntidadeNaoEncontrada(
                    f"Empréstimo {emprestimo_id} não existe"
                )

            livro_id = referencia["livro_id"]
            # Ordem global de locks do domínio: livro, depois empréstimo.
            livro = conexao.execute(
                """
                SELECT id, capacidade, disponiveis
                FROM livros
                WHERE id = %s
                FOR UPDATE
                """,
                (livro_id,),
            ).fetchone()
            emprestimo = conexao.execute(
                """
                SELECT id, livro_id, usuario_id, borrow_request_id,
                       emprestado_em, devolvido_em
                FROM emprestimos
                WHERE id = %s
                FOR UPDATE
                """,
                (emprestimo_id,),
            ).fetchone()

            erro: str | None = None
            if emprestimo["usuario_id"] != usuario_id:
                erro = "Empréstimo ativo não pertence ao usuário informado"
            elif emprestimo["devolvido_em"] is not None:
                erro = "Empréstimo já foi devolvido"

            solicitacao = conexao.execute(
                """
                INSERT INTO solicitacoes (
                    request_id, tipo, usuario_id, livro_id, emprestimo_id,
                    status, erro, payload_hash, concluido_em
                )
                VALUES (
                    %s, 'DEVOLVER', %s, %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP
                )
                RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                          ordem_fila, status, erro, payload_hash, criado_em,
                          atualizado_em, concluido_em
                """,
                (
                    request_uuid,
                    usuario_id,
                    livro_id,
                    emprestimo_id,
                    "REJEITADO" if erro else "RECEBIDO",
                    erro,
                    payload_hash,
                ),
            ).fetchone()

            if erro:
                return solicitacao

            conexao.execute(
                """
                UPDATE emprestimos
                SET devolvido_em = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (emprestimo_id,),
            )

            proximo = conexao.execute(
                """
                SELECT request_id, usuario_id, ordem_fila
                FROM solicitacoes
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status IN ('RECEBIDO', 'AGUARDANDO')
                ORDER BY ordem_fila
                LIMIT 1
                FOR UPDATE
                """,
                (livro_id,),
            ).fetchone()

            if proximo:
                novo_emprestimo = conexao.execute(
                    """
                    INSERT INTO emprestimos (
                        livro_id, usuario_id, borrow_request_id
                    )
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (livro_id, proximo["usuario_id"], proximo["request_id"]),
                ).fetchone()
                conexao.execute(
                    """
                    UPDATE solicitacoes
                    SET status = 'CONCLUIDO', emprestimo_id = %s,
                        erro = NULL, concluido_em = CURRENT_TIMESTAMP
                    WHERE request_id = %s
                    """,
                    (novo_emprestimo["id"], proximo["request_id"]),
                )
            else:
                conexao.execute(
                    """
                    UPDATE livros
                    SET disponiveis = disponiveis + 1
                    WHERE id = %s
                    """,
                    (livro_id,),
                )

            return conexao.execute(
                """
                UPDATE solicitacoes
                SET status = 'CONCLUIDO', erro = NULL,
                    concluido_em = CURRENT_TIMESTAMP
                WHERE request_id = %s
                RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                          ordem_fila, status, erro, payload_hash, criado_em,
                          atualizado_em, concluido_em
                """,
                (request_uuid,),
            ).fetchone()

