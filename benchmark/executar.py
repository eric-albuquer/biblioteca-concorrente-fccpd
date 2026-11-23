"""Executa a matriz da Etapa 6 e grava CSVs, ambiente e gráficos."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import time
from typing import Any, Callable

from benchmark.carga_distribuida import executar_carga, versoes_infraestrutura
from benchmark.comum import (
    ambiente_base,
    escrever_csv,
    escrever_json,
    resumir_repeticoes,
)
from benchmark.graficos import gerar_graficos
from benchmark.relatorio_cpu import fingerprint, processar_paralelo, processar_sequencial
from database.connection import aplicar_schema, obter_dsn
from messaging.config import obter_rabbitmq_url


CENARIOS_DISTRIBUIDOS = (
    {"volume": 100, "concorrencia": 8, "titulos": 1},
    {"volume": 100, "concorrencia": 8, "titulos": 8},
    {"volume": 500, "concorrencia": 32, "titulos": 1},
    {"volume": 500, "concorrencia": 32, "titulos": 8},
)


def executar_distribuido(
    dsn: str,
    rabbitmq_url: str,
    repeticoes: int,
    progresso: Callable[[str], None] = print,
) -> list[dict[str, Any]]:
    linhas = []
    for cenario in CENARIOS_DISTRIBUIDOS:
        for workers in (1, 2, 4):
            progresso(
                "Aquecimento distribuído: "
                f"{cenario['volume']} pedidos, {cenario['titulos']} título(s), "
                f"{workers} worker(s)"
            )
            aquecimento = executar_carga(
                dsn,
                rabbitmq_url,
                workers=workers,
                repeticao=0,
                **cenario,
            )
            if aquecimento["erros"]:
                raise RuntimeError(f"Aquecimento distribuído falhou: {aquecimento}")
            for repeticao in range(1, repeticoes + 1):
                progresso(
                    f"Distribuído {cenario}, workers={workers}, repetição={repeticao}"
                )
                linha = executar_carga(
                    dsn,
                    rabbitmq_url,
                    workers=workers,
                    repeticao=repeticao,
                    **cenario,
                )
                if linha["erros"] or linha["concluidos"] != cenario["volume"]:
                    raise RuntimeError(f"Carga distribuída incompleta: {linha}")
                linhas.append(linha)
    return linhas


def _medir_cpu(
    volume: int,
    modo: str,
    processos: int,
) -> tuple[float, str, float]:
    inicio_cpu = time.process_time()
    inicio = time.perf_counter()
    if modo == "sequencial":
        relatorio = processar_sequencial(volume)
    else:
        relatorio = processar_paralelo(volume, processos)
    duracao = time.perf_counter() - inicio
    cpu_parent = time.process_time() - inicio_cpu
    return duracao, fingerprint(relatorio), cpu_parent


def executar_cpu(
    volumes: tuple[int, ...],
    repeticoes: int,
    progresso: Callable[[str], None] = print,
) -> list[dict[str, Any]]:
    linhas = []
    configuracoes = (("sequencial", 1), ("processos", 1), ("processos", 2), ("processos", 4))
    for volume in volumes:
        esperado = fingerprint(processar_sequencial(volume))
        for modo, processos in configuracoes:
            progresso(
                f"Aquecimento CPU: volume={volume}, modo={modo}, processos={processos}"
            )
            _, fingerprint_aquecimento, _ = _medir_cpu(volume, modo, processos)
            if fingerprint_aquecimento != esperado:
                raise AssertionError("Saída do aquecimento diverge da sequencial")
            for repeticao in range(1, repeticoes + 1):
                progresso(
                    f"CPU volume={volume}, modo={modo}, processos={processos}, "
                    f"repetição={repeticao}"
                )
                duracao, resultado, cpu_parent = _medir_cpu(volume, modo, processos)
                if resultado != esperado:
                    raise AssertionError("Saída paralela diverge da sequencial")
                linhas.append(
                    {
                        "volume": volume,
                        "modo": modo,
                        "processos": processos,
                        "repeticao": repeticao,
                        "tempo_s": duracao,
                        "registros_s": volume / duracao,
                        "cpu_parent_s": cpu_parent,
                        "fingerprint": resultado,
                        "equivalente": True,
                    }
                )
    return linhas


def resumir_cpu(linhas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    resumo = resumir_repeticoes(
        linhas,
        ("volume", "modo", "processos"),
        ("tempo_s", "registros_s", "cpu_parent_s"),
    )
    for linha in resumo:
        baseline = next(
            item
            for item in resumo
            if item["volume"] == linha["volume"] and item["modo"] == "sequencial"
        )
        linha["speedup"] = baseline["tempo_s_mediana"] / linha["tempo_s_mediana"]
        linha["eficiencia"] = linha["speedup"] / int(linha["processos"])
        linha["equivalente"] = True
    return resumo


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--modo",
        choices=("todos", "distribuido", "cpu"),
        default="todos",
    )
    parser.add_argument("--repeticoes", type=int, default=5)
    parser.add_argument("--cpu-volumes", type=int, nargs="+", default=[100_000, 1_000_000])
    parser.add_argument("--saida", default="benchmark/resultados")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--rabbitmq-url", default=os.getenv("RABBITMQ_URL"))
    parser.add_argument(
        "--confirmar-banco-de-benchmark",
        action="store_true",
        help="confirma que o banco informado pode ser truncado entre rodadas",
    )
    args = parser.parse_args()
    if args.repeticoes < 1:
        parser.error("--repeticoes deve ser positivo")

    destino = Path(args.saida)
    ambiente = ambiente_base()
    if args.modo in {"todos", "distribuido"}:
        if not args.confirmar_banco_de_benchmark:
            parser.error(
                "o modo distribuído é destrutivo; use "
                "--confirmar-banco-de-benchmark em uma base isolada"
            )
        dsn = obter_dsn(args.database_url)
        rabbitmq_url = obter_rabbitmq_url(args.rabbitmq_url)
        aplicar_schema(dsn, incluir_seed=True)
        ambiente.update(versoes_infraestrutura(dsn, rabbitmq_url))
        bruto = executar_distribuido(dsn, rabbitmq_url, args.repeticoes)
        resumo = resumir_repeticoes(
            bruto,
            ("cenario", "volume", "concorrencia", "titulos", "workers"),
            (
                "tempo_aceite_s",
                "tempo_total_s",
                "throughput_aceite_ops_s",
                "throughput_conclusao_ops_s",
                "aceite_media_ms",
                "aceite_p50_ms",
                "aceite_p95_ms",
                "aceite_p99_ms",
                "conclusao_media_ms",
                "conclusao_p50_ms",
                "conclusao_p95_ms",
                "conclusao_p99_ms",
                "fila_media_ms",
                "fila_p50_ms",
                "fila_p95_ms",
                "fila_p99_ms",
                "workers_cpu_s",
                "workers_cpu_pct",
                "workers_rss_pico_mb",
                "fila_pronta_ao_concluir",
                "erros",
            ),
        )
        escrever_csv(destino / "throughput_raw.csv", bruto)
        escrever_csv(destino / "throughput_resumo.csv", resumo)

    if args.modo in {"todos", "cpu"}:
        bruto_cpu = executar_cpu(tuple(args.cpu_volumes), args.repeticoes)
        resumo_cpu = resumir_cpu(bruto_cpu)
        escrever_csv(destino / "cpu_raw.csv", bruto_cpu)
        escrever_csv(destino / "cpu_resumo.csv", resumo_cpu)

    ambiente["repeticoes_por_configuracao"] = args.repeticoes
    ambiente["aquecimentos_por_configuracao"] = 1
    escrever_json(destino / "ambiente.json", ambiente)
    graficos = gerar_graficos(destino)
    print(f"Resultados gravados em {destino.resolve()}")
    for grafico in graficos:
        print(f"Gráfico: {grafico.resolve()}")


if __name__ == "__main__":
    main()
