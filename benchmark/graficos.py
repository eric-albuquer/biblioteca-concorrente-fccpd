"""Gera gráficos SVG estáticos a partir dos resumos CSV revisados."""

from __future__ import annotations

import csv
from html import escape
from pathlib import Path
from typing import Any


CORES = ("#2563eb", "#ea580c", "#16a34a", "#7c3aed")


def _ler_csv(caminho: Path) -> list[dict[str, str]]:
    with caminho.open(encoding="utf-8", newline="") as arquivo:
        return list(csv.DictReader(arquivo))


def _barra_agrupada(
    *,
    titulo: str,
    descricao: str,
    categorias: list[str],
    series: list[tuple[str, list[float]]],
    rotulo_y: str,
) -> str:
    largura, altura = 960, 520
    esquerda, direita, topo, base = 90, 30, 80, 100
    area_largura = largura - esquerda - direita
    area_altura = altura - topo - base
    maximo = max((valor for _, valores in series for valor in valores), default=1)
    maximo = max(maximo, 1) * 1.15
    grupo = area_largura / max(len(categorias), 1)
    largura_barra = min(54, grupo * 0.7 / max(len(series), 1))
    elementos = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{largura}" '
        f'height="{altura}" viewBox="0 0 {largura} {altura}" role="img">',
        f"<title>{escape(titulo)}</title>",
        f"<desc>{escape(descricao)}</desc>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{esquerda}" y="35" font-size="22" font-family="sans-serif" '
        f'font-weight="700" fill="#111827">{escape(titulo)}</text>',
    ]
    for marca in range(6):
        valor = maximo * marca / 5
        y = topo + area_altura - area_altura * marca / 5
        elementos.extend(
            [
                f'<line x1="{esquerda}" y1="{y:.1f}" x2="{largura-direita}" '
                f'y2="{y:.1f}" stroke="#d1d5db" stroke-width="1"/>',
                f'<text x="{esquerda-10}" y="{y+4:.1f}" text-anchor="end" '
                f'font-size="12" font-family="sans-serif" fill="#374151">{valor:.1f}</text>',
            ]
        )
    for indice_categoria, categoria in enumerate(categorias):
        centro = esquerda + grupo * (indice_categoria + 0.5)
        total_barras = largura_barra * len(series)
        inicio = centro - total_barras / 2
        for indice_serie, (_, valores) in enumerate(series):
            valor = valores[indice_categoria]
            h = area_altura * valor / maximo
            x = inicio + indice_serie * largura_barra
            y = topo + area_altura - h
            cor = CORES[indice_serie % len(CORES)]
            elementos.extend(
                [
                    f'<rect x="{x+2:.1f}" y="{y:.1f}" width="{largura_barra-4:.1f}" '
                    f'height="{h:.1f}" fill="{cor}" rx="3"/>',
                    f'<text x="{x+largura_barra/2:.1f}" y="{max(y-6, topo+12):.1f}" '
                    f'text-anchor="middle" font-size="11" font-family="sans-serif" '
                    f'fill="#111827">{valor:.2f}</text>',
                ]
            )
        elementos.append(
            f'<text x="{centro:.1f}" y="{topo+area_altura+24}" text-anchor="middle" '
            f'font-size="13" font-family="sans-serif" fill="#111827">{escape(categoria)}</text>'
        )
    elementos.append(
        f'<text transform="translate(22 {topo+area_altura/2}) rotate(-90)" '
        f'text-anchor="middle" font-size="13" font-family="sans-serif" '
        f'fill="#111827">{escape(rotulo_y)}</text>'
    )
    legenda_x = esquerda
    legenda_y = altura - 34
    for indice, (nome, _) in enumerate(series):
        x = legenda_x + indice * 210
        cor = CORES[indice % len(CORES)]
        elementos.extend(
            [
                f'<rect x="{x}" y="{legenda_y-12}" width="16" height="16" fill="{cor}"/>',
                f'<text x="{x+24}" y="{legenda_y+1}" font-size="13" '
                f'font-family="sans-serif" fill="#111827">{escape(nome)}</text>',
            ]
        )
    elementos.append("</svg>")
    return "\n".join(elementos)


def gerar_graficos(diretorio: str | Path) -> list[Path]:
    raiz = Path(diretorio)
    destino = raiz / "graficos"
    destino.mkdir(parents=True, exist_ok=True)
    gerados: list[Path] = []

    throughput_path = raiz / "throughput_resumo.csv"
    if throughput_path.exists():
        linhas = _ler_csv(throughput_path)
        maior_volume = max(int(linha["volume"]) for linha in linhas)
        concorrencia = max(
            int(linha["concorrencia"])
            for linha in linhas
            if int(linha["volume"]) == maior_volume
        )
        recorte = [
            linha
            for linha in linhas
            if int(linha["volume"]) == maior_volume
            and int(linha["concorrencia"]) == concorrencia
        ]
        workers = sorted({int(linha["workers"]) for linha in recorte})
        quantidades_titulos = sorted({int(linha["titulos"]) for linha in recorte})
        series = []
        for titulos in quantidades_titulos:
            valores = []
            for quantidade_workers in workers:
                linha = next(
                    item
                    for item in recorte
                    if int(item["workers"]) == quantidade_workers
                    and int(item["titulos"]) == titulos
                )
                valores.append(float(linha["throughput_conclusao_ops_s_mediana"]))
            series.append((f"{titulos} título(s)", valores))
        arquivo = destino / "throughput_workers.svg"
        arquivo.write_text(
            _barra_agrupada(
                titulo=f"Throughput com {maior_volume} pedidos",
                descricao=(
                    "Mediana de cinco repetições por quantidade de workers; "
                    "barras comparam contenção em um título e distribuição."
                ),
                categorias=[f"{worker} worker(s)" for worker in workers],
                series=series,
                rotulo_y="Comandos concluídos por segundo",
            ),
            encoding="utf-8",
        )
        gerados.append(arquivo)

    cpu_path = raiz / "cpu_resumo.csv"
    if cpu_path.exists():
        linhas = _ler_csv(cpu_path)
        volumes = sorted({int(linha["volume"]) for linha in linhas})
        processos = sorted(
            {
                int(linha["processos"])
                for linha in linhas
                if linha["modo"] == "processos"
            }
        )
        series = []
        for volume in volumes:
            valores = []
            for quantidade_processos in processos:
                linha = next(
                    item
                    for item in linhas
                    if int(item["volume"]) == volume
                    and item["modo"] == "processos"
                    and int(item["processos"]) == quantidade_processos
                )
                valores.append(float(linha["speedup"]))
            series.append((f"{volume:,} registros".replace(",", "."), valores))
        arquivo = destino / "speedup_cpu.svg"
        arquivo.write_text(
            _barra_agrupada(
                titulo="Speedup do relatório CPU-bound",
                descricao=(
                    "Tempo sequencial mediano dividido pelo tempo paralelo mediano; "
                    "valores abaixo de um indicam regressão."
                ),
                categorias=[f"{processo} processo(s)" for processo in processos],
                series=series,
                rotulo_y="Speedup (vezes)",
            ),
            encoding="utf-8",
        )
        gerados.append(arquivo)
    return gerados

