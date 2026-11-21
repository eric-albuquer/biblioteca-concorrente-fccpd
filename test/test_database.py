from __future__ import annotations

import pytest
from psycopg.errors import CheckViolation, UniqueViolation

from database.connection import aplicar_schema, conectar, executar_sql
from database.repository import RepositorioBiblioteca


def test_seed_e_catalogo_respeitam_capacidades(banco_limpo):
    repositorio = RepositorioBiblioteca(banco_limpo)

    livros = [repositorio.consultar_livro(livro_id) for livro_id in (1, 2, 3)]

    assert [livro["titulo"] for livro in livros] == ["Duna", "1984", "O Hobbit"]
    assert [(livro["capacidade"], livro["disponiveis"]) for livro in livros] == [
        (3, 3),
        (2, 2),
        (4, 4),
    ]


def test_schema_e_seed_podem_ser_reaplicados(banco_limpo):
    aplicar_schema(banco_limpo)
    executar_sql("database/seed.sql", banco_limpo)

    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM livros"
        ).fetchone()["total"] == 3
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM usuarios"
        ).fetchone()["total"] == 4


@pytest.mark.parametrize("disponiveis", [-1, 4])
def test_constraint_impede_estoque_fora_dos_limites(banco_limpo, disponiveis):
    with pytest.raises(CheckViolation):
        with conectar(banco_limpo) as conexao:
            conexao.execute(
                "UPDATE livros SET disponiveis = %s WHERE id = 1",
                (disponiveis,),
            )


def test_ordem_fifo_e_unica_por_livro_no_banco(banco_limpo):
    with conectar(banco_limpo) as conexao:
        conexao.execute(
            """
            INSERT INTO solicitacoes (
                request_id, tipo, usuario_id, livro_id, ordem_fila,
                status, payload_hash
            ) VALUES
                ('00000000-0000-0000-0000-000000000001', 'PEDIR', 1, 1, 1,
                 'RECEBIDO', repeat('a', 64))
            """
        )

    with pytest.raises(UniqueViolation):
        with conectar(banco_limpo) as conexao:
            conexao.execute(
                """
                INSERT INTO solicitacoes (
                    request_id, tipo, usuario_id, livro_id, ordem_fila,
                    status, payload_hash
                ) VALUES
                    ('00000000-0000-0000-0000-000000000002', 'PEDIR', 2, 1, 1,
                     'RECEBIDO', repeat('b', 64))
                """
            )


def test_borrow_request_id_nao_pode_gerar_dois_emprestimos(banco_limpo):
    request_id = "00000000-0000-0000-0000-000000000001"
    with conectar(banco_limpo) as conexao:
        conexao.execute(
            """
            INSERT INTO solicitacoes (
                request_id, tipo, usuario_id, livro_id, ordem_fila,
                status, payload_hash
            ) VALUES (%s, 'PEDIR', 1, 1, 1, 'RECEBIDO', repeat('a', 64))
            """,
            (request_id,),
        )
        conexao.execute(
            """
            INSERT INTO emprestimos (livro_id, usuario_id, borrow_request_id)
            VALUES (1, 1, %s)
            """,
            (request_id,),
        )

    with pytest.raises(UniqueViolation):
        with conectar(banco_limpo) as conexao:
            conexao.execute(
                """
                INSERT INTO emprestimos (livro_id, usuario_id, borrow_request_id)
                VALUES (1, 1, %s)
                """,
                (request_id,),
            )

