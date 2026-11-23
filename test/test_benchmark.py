from __future__ import annotations

from benchmark.carga_distribuida import executar_carga, estado_rabbit
from benchmark.comum import percentil, resumir_repeticoes
from benchmark.executar import resumir_cpu
from benchmark.relatorio_cpu import (
    fingerprint,
    processar_paralelo,
    processar_sequencial,
)


def test_percentil_nearest_rank_preserva_observacoes():
    valores = [50, 10, 40, 20, 30]
    assert percentil(valores, 50) == 30
    assert percentil(valores, 95) == 50
    assert percentil(valores, 99) == 50


def test_relatorio_sequencial_e_paralelo_sao_equivalentes():
    sequencial = processar_sequencial(
        10_000,
        seed=123,
        quantidade_titulos=20,
        quantidade_usuarios=100,
    )
    for processos in (1, 2, 4):
        paralelo = processar_paralelo(
            10_000,
            processos,
            seed=123,
            quantidade_titulos=20,
            quantidade_usuarios=100,
        )
        assert paralelo == sequencial
        assert fingerprint(paralelo) == fingerprint(sequencial)


def test_resumo_registra_mediana_variacao_speedup_e_eficiencia():
    linhas = []
    for repeticao, tempo in enumerate((2.0, 1.8, 2.2, 2.1, 1.9), start=1):
        linhas.append(
            {
                "volume": 100,
                "modo": "sequencial",
                "processos": 1,
                "repeticao": repeticao,
                "tempo_s": tempo,
                "registros_s": 100 / tempo,
                "cpu_parent_s": tempo,
            }
        )
    for repeticao, tempo in enumerate((1.0, 0.9, 1.1, 1.0, 1.0), start=1):
        linhas.append(
            {
                "volume": 100,
                "modo": "processos",
                "processos": 2,
                "repeticao": repeticao,
                "tempo_s": tempo,
                "registros_s": 100 / tempo,
                "cpu_parent_s": 0.1,
            }
        )

    resumo = resumir_cpu(linhas)
    paralelo = next(linha for linha in resumo if linha["modo"] == "processos")
    assert paralelo["repeticoes"] == 5
    assert paralelo["tempo_s_mediana"] == 1.0
    assert paralelo["tempo_s_desvio"] > 0
    assert paralelo["speedup"] == 2.0
    assert paralelo["eficiencia"] == 1.0


def test_resumo_generico_nao_mistura_configuracoes():
    linhas = [
        {"grupo": "a", "valor": 1.0},
        {"grupo": "a", "valor": 3.0},
        {"grupo": "b", "valor": 10.0},
        {"grupo": "b", "valor": 14.0},
    ]
    resumo = resumir_repeticoes(linhas, ("grupo",), ("valor",))
    assert resumo[0]["valor_media"] == 2.0
    assert resumo[1]["valor_mediana"] == 12.0


def test_carga_distribuida_curta_conclui_sem_perder_comandos(
    banco_limpo, rabbitmq_limpo
):
    resultado = executar_carga(
        banco_limpo,
        rabbitmq_limpo,
        volume=20,
        concorrencia=4,
        titulos=2,
        workers=2,
        repeticao=1,
    )

    assert resultado["aceitos"] == 20
    assert resultado["concluidos"] == 20
    assert resultado["erros"] == 0
    assert resultado["throughput_aceite_ops_s"] > 0
    assert resultado["throughput_conclusao_ops_s"] > 0
    assert resultado["conclusao_p99_ms"] >= resultado["conclusao_p95_ms"]
    assert resultado["workers_rss_pico_mb"] > 0
    assert estado_rabbit(rabbitmq_limpo) == {
        "mensagens": 0,
        "consumidores": 0,
        "dlq": 0,
    }
