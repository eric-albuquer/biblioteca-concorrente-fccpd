"""Empréstimos persistentes, idempotentes e FIFO por livro."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from database.connection import transacao
from services.comum import (
    ConflitoIdempotencia,
    EntidadeNaoEncontrada,
    RegraDeNegocioError,
    bloquear_request_id,
    hash_payload,
    normalizar_uuid,
    selecionar_solicitacao,
    validar_repeticao,
)


class ServicoEmprestimos:
    """Coordena transações sem manter estoque ou fila na memória do processo."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        failpoint: Callable[[str, str], None] | None = None,
    ):
        self.dsn = dsn
        self.failpoint = failpoint

    def registrar_pedido(
        self, request_id: UUID | str, usuario_id: int, livro_id: int
    ) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        payload_hash = hash_payload(
            {"tipo": "PEDIR", "usuario_id": usuario_id, "livro_id": livro_id}
        )

        with transacao(self.dsn) as conexao:
            # Serializa somente retransmissões do mesmo UUID, inclusive se elas
            # apontarem por engano para livros diferentes.
            bloquear_request_id(conexao, request_uuid)
            existente = selecionar_solicitacao(conexao, request_uuid)
            if existente:
                return validar_repeticao(
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

    def processar_pedidos(
        self,
        livro_id: int,
        *,
        gatilho_request_id: UUID | str | None = None,
    ) -> list[dict[str, Any]]:
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
            if gatilho_request_id is not None:
                self._executar_failpoint(
                    "BEFORE_COMMIT", str(gatilho_request_id)
                )

        return atendidos

    def registrar_e_processar_pedido(
        self, request_id: UUID | str, usuario_id: int, livro_id: int
    ) -> dict[str, Any]:
        solicitacao = self.registrar_pedido(request_id, usuario_id, livro_id)
        self.processar_pedidos(livro_id)
        with transacao(self.dsn) as conexao:
            return selecionar_solicitacao(
                conexao, normalizar_uuid(solicitacao["request_id"])
            )

    def devolver_emprestimo(
        self, request_id: UUID | str, usuario_id: int, emprestimo_id: int
    ) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        payload_hash = hash_payload(
            {
                "tipo": "DEVOLVER",
                "usuario_id": usuario_id,
                "emprestimo_id": emprestimo_id,
            }
        )

        with transacao(self.dsn) as conexao:
            bloquear_request_id(conexao, request_uuid)
            existente = selecionar_solicitacao(conexao, request_uuid)
            if existente:
                solicitacao = validar_repeticao(
                    existente, tipo="DEVOLVER", payload_hash=payload_hash
                )
                if solicitacao["status"] != "RECEBIDO":
                    return solicitacao
                return self._aplicar_devolucao(conexao, solicitacao)

            referencia = conexao.execute(
                "SELECT livro_id FROM emprestimos WHERE id = %s",
                (emprestimo_id,),
            ).fetchone()
            if not referencia:
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
                    referencia["livro_id"],
                    emprestimo_id,
                    payload_hash,
                ),
            ).fetchone()
            return self._aplicar_devolucao(conexao, solicitacao)

    def processar_comando(
        self,
        request_id: UUID | str,
        tipo_esperado: str | None = None,
    ) -> dict[str, Any]:
        """Aplica a solicitação canônica do banco e tolera redelivery."""
        request_uuid = normalizar_uuid(request_id)
        with transacao(self.dsn) as conexao:
            solicitacao = selecionar_solicitacao(conexao, request_uuid)
            if not solicitacao:
                raise EntidadeNaoEncontrada(
                    f"Solicitação {request_uuid} não existe"
                )
            tipo = solicitacao["tipo"]
            livro_id = solicitacao["livro_id"]
            if tipo_esperado is not None and tipo != tipo_esperado:
                raise RegraDeNegocioError(
                    f"Mensagem {tipo_esperado} diverge da solicitação {tipo}"
                )

        if tipo == "PEDIR":
            self.processar_pedidos(
                livro_id,
                gatilho_request_id=request_uuid,
            )
        elif tipo == "DEVOLVER":
            return self.processar_devolucao(request_uuid)
        else:
            raise RegraDeNegocioError(f"Tipo de solicitação inválido: {tipo}")

        with transacao(self.dsn) as conexao:
            return selecionar_solicitacao(conexao, request_uuid)

    def processar_devolucao(
        self, request_id: UUID | str
    ) -> dict[str, Any]:
        request_uuid = normalizar_uuid(request_id)
        with transacao(self.dsn) as conexao:
            solicitacao = selecionar_solicitacao(conexao, request_uuid)
            if not solicitacao:
                raise EntidadeNaoEncontrada(
                    f"Solicitação {request_uuid} não existe"
                )
            if solicitacao["tipo"] != "DEVOLVER":
                raise RegraDeNegocioError(
                    f"Solicitação {request_uuid} não é uma devolução"
                )
            if solicitacao["status"] != "RECEBIDO":
                return solicitacao
            return self._aplicar_devolucao(conexao, solicitacao)

    def _aplicar_devolucao(
        self, conexao, solicitacao: dict[str, Any]
    ) -> dict[str, Any]:
        livro_id = solicitacao["livro_id"]
        emprestimo_id = solicitacao["emprestimo_id"]
        request_id = solicitacao["request_id"]

        # Ordem global de locks: livro, solicitação, empréstimo.
        conexao.execute(
            "SELECT id FROM livros WHERE id = %s FOR UPDATE",
            (livro_id,),
        ).fetchone()
        atual = conexao.execute(
            """
            SELECT request_id, tipo, usuario_id, livro_id, emprestimo_id,
                   ordem_fila, status, erro, payload_hash, criado_em,
                   atualizado_em, concluido_em
            FROM solicitacoes
            WHERE request_id = %s
            FOR UPDATE
            """,
            (request_id,),
        ).fetchone()
        if atual["status"] != "RECEBIDO":
            return atual

        emprestimo = conexao.execute(
            """
            SELECT id, usuario_id, devolvido_em
            FROM emprestimos
            WHERE id = %s
            FOR UPDATE
            """,
            (emprestimo_id,),
        ).fetchone()

        erro: str | None = None
        if not emprestimo:
            erro = "Empréstimo não existe"
        elif emprestimo["usuario_id"] != atual["usuario_id"]:
            erro = "Empréstimo ativo não pertence ao usuário informado"
        elif emprestimo["devolvido_em"] is not None:
            erro = "Empréstimo já foi devolvido"

        if erro:
            resultado = conexao.execute(
                """
                UPDATE solicitacoes
                SET status = 'REJEITADO', erro = %s,
                    concluido_em = CURRENT_TIMESTAMP
                WHERE request_id = %s
                RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                          ordem_fila, status, erro, payload_hash, criado_em,
                          atualizado_em, concluido_em
                """,
                (erro, request_id),
            ).fetchone()
            self._executar_failpoint("BEFORE_COMMIT", str(request_id))
            return resultado

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

        resultado = conexao.execute(
            """
            UPDATE solicitacoes
            SET status = 'CONCLUIDO', erro = NULL,
                concluido_em = CURRENT_TIMESTAMP
            WHERE request_id = %s
            RETURNING request_id, tipo, usuario_id, livro_id, emprestimo_id,
                      ordem_fila, status, erro, payload_hash, criado_em,
                      atualizado_em, concluido_em
            """,
            (request_id,),
        ).fetchone()
        self._executar_failpoint("BEFORE_COMMIT", str(request_id))
        return resultado

    def _executar_failpoint(self, nome: str, request_id: str) -> None:
        if self.failpoint is not None:
            self.failpoint(nome, request_id)

