from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from database.connection import conectar
from services.comum import ConflitoIdempotencia
from services.emprestimos import ServicoEmprestimos
from services.solicitacoes import ServicoSolicitacoes


def _contagens(dsn: str) -> tuple[int, int, int]:
    with conectar(dsn) as conexao:
        return (
            conexao.execute(
                "SELECT COUNT(*) AS total FROM solicitacoes"
            ).fetchone()["total"],
            conexao.execute("SELECT COUNT(*) AS total FROM outbox").fetchone()[
                "total"
            ],
            conexao.execute(
                "SELECT COUNT(*) AS total FROM emprestimos"
            ).fetchone()["total"],
        )


def test_aceite_persiste_solicitacao_e_outbox_sem_processar_emprestimo(banco_limpo):
    servico = ServicoSolicitacoes(banco_limpo)
    request_id = uuid4()

    aceite = servico.aceitar_pedido(request_id, usuario_id=1, livro_id=1)
    recuperada = ServicoSolicitacoes(banco_limpo).consultar_status(request_id)

    assert aceite["status"] == recuperada["status"] == "RECEBIDO"
    assert aceite["ordem_fila"] == 1
    assert _contagens(banco_limpo) == (1, 1, 0)

    with conectar(banco_limpo) as conexao:
        evento = conexao.execute(
            "SELECT event_type, payload FROM outbox WHERE request_id = %s",
            (request_id,),
        ).fetchone()
        livro = conexao.execute(
            "SELECT capacidade, disponiveis FROM livros WHERE id = 1"
        ).fetchone()
    assert evento["event_type"] == "PEDIR"
    assert evento["payload"]["request_id"] == str(request_id)
    assert livro["disponiveis"] == livro["capacidade"] == 3


def test_retransmissoes_concorrentes_criam_uma_solicitacao_e_um_evento(
    banco_limpo,
):
    servico = ServicoSolicitacoes(banco_limpo)
    request_id = uuid4()

    with ThreadPoolExecutor(max_workers=8) as executor:
        respostas = list(
            executor.map(
                lambda _: servico.aceitar_pedido(request_id, 1, 1),
                range(8),
            )
        )

    assert {resposta["ordem_fila"] for resposta in respostas} == {1}
    assert _contagens(banco_limpo) == (1, 1, 0)
    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT ultimo_seq_pedido FROM livros WHERE id = 1"
        ).fetchone()["ultimo_seq_pedido"] == 1


def test_mesmo_uuid_com_payload_diferente_nao_cria_evento(banco_limpo):
    servico = ServicoSolicitacoes(banco_limpo)
    request_id = uuid4()
    servico.aceitar_pedido(request_id, 1, 1)

    with pytest.raises(ConflitoIdempotencia):
        servico.aceitar_pedido(request_id, 2, 1)

    assert _contagens(banco_limpo) == (1, 1, 0)


def test_falha_da_outbox_reverte_solicitacao_e_sequencia(banco_limpo, monkeypatch):
    servico = ServicoSolicitacoes(banco_limpo)

    def falhar(*args, **kwargs):
        raise RuntimeError("falha controlada da outbox")

    monkeypatch.setattr(servico, "_inserir_outbox", falhar)
    with pytest.raises(RuntimeError, match="falha controlada"):
        servico.aceitar_pedido(uuid4(), 1, 1)

    assert _contagens(banco_limpo) == (0, 0, 0)
    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT ultimo_seq_pedido FROM livros WHERE id = 1"
        ).fetchone()["ultimo_seq_pedido"] == 0


def test_aceite_de_devolucao_tambem_e_atomico_e_nao_aplica_dominio(banco_limpo):
    dominio = ServicoEmprestimos(banco_limpo)
    pedido = dominio.registrar_e_processar_pedido(uuid4(), 1, 1)
    emprestimo_id = pedido["emprestimo_id"]
    servico = ServicoSolicitacoes(banco_limpo)

    devolucao = servico.aceitar_devolucao(uuid4(), 1, emprestimo_id)

    assert devolucao["status"] == "RECEBIDO"
    with conectar(banco_limpo) as conexao:
        emprestimo = conexao.execute(
            "SELECT devolvido_em FROM emprestimos WHERE id = %s",
            (emprestimo_id,),
        ).fetchone()
        evento = conexao.execute(
            "SELECT event_type, payload FROM outbox WHERE request_id = %s",
            (devolucao["request_id"],),
        ).fetchone()
    assert emprestimo["devolvido_em"] is None
    assert evento["event_type"] == "DEVOLVER"
    assert evento["payload"]["emprestimo_id"] == emprestimo_id

