from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from database.connection import conectar
from database.repository import RepositorioBiblioteca
from services.emprestimos import ConflitoIdempotencia, ServicoEmprestimos


def _configurar_livro_com_um_exemplar(dsn: str, usuarios: int = 8) -> None:
    with conectar(dsn) as conexao:
        conexao.execute(
            "UPDATE livros SET capacidade = 1, disponiveis = 1 WHERE id = 1"
        )
        with conexao.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO usuarios (id, nome)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                [(numero, f"Usuário {numero}") for numero in range(1, usuarios + 1)],
            )


def test_disputa_concorrente_preserva_estoque_e_fifo(banco_limpo):
    _configurar_livro_com_um_exemplar(banco_limpo)
    servico = ServicoEmprestimos(banco_limpo)
    repositorio = RepositorioBiblioteca(banco_limpo)
    pedidos = [(usuario_id, uuid4()) for usuario_id in range(1, 9)]

    def pedir(item):
        usuario_id, request_id = item
        return servico.registrar_e_processar_pedido(request_id, usuario_id, 1)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(pedir, pedidos))

    with conectar(banco_limpo) as conexao:
        solicitacoes = conexao.execute(
            """
            SELECT request_id, usuario_id, ordem_fila, status, emprestimo_id
            FROM solicitacoes
            WHERE livro_id = 1 AND tipo = 'PEDIR'
            ORDER BY ordem_fila
            """
        ).fetchall()

    assert [linha["ordem_fila"] for linha in solicitacoes] == list(range(1, 9))
    assert [linha["status"] for linha in solicitacoes].count("CONCLUIDO") == 1
    assert [linha["status"] for linha in solicitacoes].count("AGUARDANDO") == 7
    assert solicitacoes[0]["status"] == "CONCLUIDO"
    assert repositorio.consultar_livro(1)["disponiveis"] == 0
    assert len(repositorio.listar_emprestimos_ativos(1)) == 1
    assert repositorio.resumo_invariantes()[0]["estoque_consistente"] is True

    # Cada devolução transfere a unidade diretamente ao próximo da fila.
    ordem_atendida = [solicitacoes[0]["request_id"]]
    for _ in range(7):
        ativo = repositorio.listar_emprestimos_ativos(1)[0]
        servico.devolver_emprestimo(uuid4(), ativo["usuario_id"], ativo["id"])
        novo_ativo = repositorio.listar_emprestimos_ativos(1)[0]
        ordem_atendida.append(novo_ativo["borrow_request_id"])
        assert repositorio.consultar_livro(1)["disponiveis"] == 0

    assert ordem_atendida == [linha["request_id"] for linha in solicitacoes]

    ultimo = repositorio.listar_emprestimos_ativos(1)[0]
    servico.devolver_emprestimo(uuid4(), ultimo["usuario_id"], ultimo["id"])
    assert repositorio.listar_emprestimos_ativos(1) == []
    assert repositorio.consultar_livro(1)["disponiveis"] == 1
    assert repositorio.resumo_invariantes()[0]["estoque_consistente"] is True


def test_entrega_disparada_fora_de_ordem_atende_o_mais_antigo(banco_limpo):
    _configurar_livro_com_um_exemplar(banco_limpo, usuarios=3)
    servico = ServicoEmprestimos(banco_limpo)
    ids = [uuid4(), uuid4(), uuid4()]

    solicitacoes = [
        servico.registrar_pedido(request_id, indice, 1)
        for indice, request_id in enumerate(ids, start=1)
    ]

    # O gatilho poderia representar a mensagem do terceiro pedido. O serviço
    # processa o livro e escolhe pelo banco, nunca pela ordem de entrega.
    servico.processar_pedidos(solicitacoes[2]["livro_id"])

    estados = [
        RepositorioBiblioteca(banco_limpo).consultar_solicitacao(request_id)
        for request_id in ids
    ]
    assert [estado["status"] for estado in estados] == [
        "CONCLUIDO",
        "AGUARDANDO",
        "AGUARDANDO",
    ]


def test_devolucao_valida_posse_e_nao_infla_estoque(banco_limpo):
    _configurar_livro_com_um_exemplar(banco_limpo, usuarios=2)
    servico = ServicoEmprestimos(banco_limpo)
    repositorio = RepositorioBiblioteca(banco_limpo)
    pedido = servico.registrar_e_processar_pedido(uuid4(), 1, 1)
    emprestimo_id = pedido["emprestimo_id"]

    indevida = servico.devolver_emprestimo(uuid4(), 2, emprestimo_id)
    assert indevida["status"] == "REJEITADO"
    assert repositorio.consultar_livro(1)["disponiveis"] == 0
    assert len(repositorio.listar_emprestimos_ativos(1)) == 1

    devolucao_id = uuid4()
    valida = servico.devolver_emprestimo(devolucao_id, 1, emprestimo_id)
    repetida_mesmo_id = servico.devolver_emprestimo(devolucao_id, 1, emprestimo_id)
    segunda_devolucao = servico.devolver_emprestimo(uuid4(), 1, emprestimo_id)

    assert valida["status"] == "CONCLUIDO"
    assert repetida_mesmo_id["status"] == "CONCLUIDO"
    assert segunda_devolucao["status"] == "REJEITADO"
    assert repositorio.consultar_livro(1)["disponiveis"] == 1
    assert repositorio.listar_emprestimos_ativos(1) == []


def test_request_id_repetido_e_idempotente_mas_payload_diferente_conflita(
    banco_limpo,
):
    servico = ServicoEmprestimos(banco_limpo)
    request_id = uuid4()

    primeira = servico.registrar_pedido(request_id, 1, 1)
    repetida = servico.registrar_pedido(request_id, 1, 1)

    assert primeira["request_id"] == repetida["request_id"]
    assert primeira["ordem_fila"] == repetida["ordem_fila"] == 1

    with pytest.raises(ConflitoIdempotencia):
        servico.registrar_pedido(request_id, 2, 1)

    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM solicitacoes"
        ).fetchone()["total"] == 1

