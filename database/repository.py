"""Consultas de leitura usadas por serviços, testes e verificação de estado."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from database.connection import conectar


class RepositorioBiblioteca:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn

    def consultar_solicitacao(self, request_id: UUID | str) -> dict[str, Any] | None:
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT request_id, tipo, usuario_id, livro_id, emprestimo_id,
                       ordem_fila, status, erro, payload_hash, criado_em,
                       atualizado_em, concluido_em
                FROM solicitacoes
                WHERE request_id = %s
                """,
                (str(request_id),),
            ).fetchone()

    def consultar_livro(self, livro_id: int) -> dict[str, Any] | None:
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT id, titulo, autor, edicao, genero, ano_publicacao, preco,
                       capacidade, disponiveis, ultimo_seq_pedido
                FROM livros
                WHERE id = %s
                """,
                (livro_id,),
            ).fetchone()

    def listar_fila(self, livro_id: int) -> list[dict[str, Any]]:
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT request_id, usuario_id, ordem_fila, status, criado_em
                FROM solicitacoes
                WHERE livro_id = %s
                  AND tipo = 'PEDIR'
                  AND status IN ('RECEBIDO', 'AGUARDANDO')
                ORDER BY ordem_fila
                """,
                (livro_id,),
            ).fetchall()

    def listar_emprestimos_ativos(self, livro_id: int) -> list[dict[str, Any]]:
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT id, livro_id, usuario_id, borrow_request_id, emprestado_em
                FROM emprestimos
                WHERE livro_id = %s AND devolvido_em IS NULL
                ORDER BY id
                """,
                (livro_id,),
            ).fetchall()

    def resumo_invariantes(self) -> list[dict[str, Any]]:
        """Retorna o estado necessário para auditar a invariante de estoque."""
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT l.id AS livro_id,
                       l.titulo,
                       l.capacidade,
                       l.disponiveis,
                       COUNT(e.id) FILTER (WHERE e.devolvido_em IS NULL)::int
                           AS emprestimos_ativos,
                       l.disponiveis
                           + COUNT(e.id) FILTER (WHERE e.devolvido_em IS NULL)
                           = l.capacidade AS estoque_consistente
                FROM livros AS l
                LEFT JOIN emprestimos AS e ON e.livro_id = l.id
                GROUP BY l.id
                ORDER BY l.id
                """
            ).fetchall()

