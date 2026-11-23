"""Relatório CPU-bound sequencial e paralelo sobre histórico determinístico."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import math
from typing import Any


MASK_64 = (1 << 64) - 1
MAX_ESPERA_MINUTOS = 24 * 60


def _misturar(valor: int) -> int:
    valor = (valor + 0x9E3779B97F4A7C15) & MASK_64
    valor = ((valor ^ (valor >> 30)) * 0xBF58476D1CE4E5B9) & MASK_64
    valor = ((valor ^ (valor >> 27)) * 0x94D049BB133111EB) & MASK_64
    return valor ^ (valor >> 31)


def _processar_intervalo(argumentos: tuple[int, int, int, int, int]) -> dict:
    inicio, fim, seed, quantidade_titulos, quantidade_usuarios = argumentos
    titulos = [[0, 0, 0, 0] for _ in range(quantidade_titulos)]
    usuarios = [[0, 0] for _ in range(quantidade_usuarios)]
    histograma = [0] * (MAX_ESPERA_MINUTOS + 1)
    checksum = 0
    atrasados = 0

    for indice in range(inicio, fim):
        primeiro = _misturar(seed + indice)
        segundo = _misturar(primeiro)
        livro = primeiro % quantidade_titulos
        usuario = (primeiro >> 32) % quantidade_usuarios
        espera = segundo % (MAX_ESPERA_MINUTOS + 1)
        duracao_horas = 1 + ((segundo >> 32) % (24 * 60))
        atrasado = duracao_horas > 24 * 30

        titulo = titulos[livro]
        titulo[0] += 1
        titulo[1] += espera
        titulo[2] = max(titulo[2], espera)
        titulo[3] += int(espera > 60)
        usuarios[usuario][0] += 1
        usuarios[usuario][1] += int(atrasado)
        histograma[espera] += 1
        atrasados += int(atrasado)
        checksum = (
            checksum
            + (livro + 1) * 31
            + (usuario + 1) * 17
            + espera * 13
            + duracao_horas * 7
        ) & MASK_64

    return {
        "total": fim - inicio,
        "titulos": titulos,
        "usuarios": usuarios,
        "histograma": histograma,
        "atrasados": atrasados,
        "checksum": checksum,
    }


def _combinar(partes: list[dict]) -> dict:
    combinado = partes[0]
    for parte in partes[1:]:
        combinado["total"] += parte["total"]
        combinado["atrasados"] += parte["atrasados"]
        combinado["checksum"] = (
            combinado["checksum"] + parte["checksum"]
        ) & MASK_64
        for destino, origem in zip(combinado["titulos"], parte["titulos"]):
            destino[0] += origem[0]
            destino[1] += origem[1]
            destino[2] = max(destino[2], origem[2])
            destino[3] += origem[3]
        for destino, origem in zip(combinado["usuarios"], parte["usuarios"]):
            destino[0] += origem[0]
            destino[1] += origem[1]
        for indice, quantidade in enumerate(parte["histograma"]):
            combinado["histograma"][indice] += quantidade
    return combinado


def _percentil_histograma(histograma: list[int], percentual: float) -> int:
    alvo = math.ceil(sum(histograma) * percentual / 100)
    acumulado = 0
    for valor, quantidade in enumerate(histograma):
        acumulado += quantidade
        if acumulado >= alvo:
            return valor
    return len(histograma) - 1


def _finalizar(parcial: dict) -> dict[str, Any]:
    total = parcial["total"]
    total_espera = sum(
        valor * quantidade
        for valor, quantidade in enumerate(parcial["histograma"])
    )
    ranking_titulos = sorted(
        (
            {
                "livro_id": indice + 1,
                "emprestimos": valores[0],
                "espera_media_min": valores[1] / valores[0] if valores[0] else 0,
                "espera_max_min": valores[2],
                "acima_sla_60_min": valores[3],
            }
            for indice, valores in enumerate(parcial["titulos"])
        ),
        key=lambda linha: (-linha["emprestimos"], linha["livro_id"]),
    )[:10]
    ranking_usuarios = sorted(
        (
            {
                "usuario_id": indice + 1,
                "emprestimos": valores[0],
                "atrasados": valores[1],
            }
            for indice, valores in enumerate(parcial["usuarios"])
        ),
        key=lambda linha: (-linha["emprestimos"], linha["usuario_id"]),
    )[:10]
    return {
        "registros": total,
        "espera_media_min": total_espera / total if total else 0,
        "espera_p50_min": _percentil_histograma(parcial["histograma"], 50),
        "espera_p95_min": _percentil_histograma(parcial["histograma"], 95),
        "espera_p99_min": _percentil_histograma(parcial["histograma"], 99),
        "dentro_sla_60_min": sum(parcial["histograma"][:61]),
        "emprestimos_atrasados": parcial["atrasados"],
        "ranking_titulos": ranking_titulos,
        "ranking_usuarios": ranking_usuarios,
        "checksum": parcial["checksum"],
    }


def processar_sequencial(
    volume: int,
    *,
    seed: int = 20261121,
    quantidade_titulos: int = 200,
    quantidade_usuarios: int = 2000,
) -> dict[str, Any]:
    if volume < 1:
        raise ValueError("volume deve ser positivo")
    parcial = _processar_intervalo(
        (0, volume, seed, quantidade_titulos, quantidade_usuarios)
    )
    return _finalizar(parcial)


def processar_paralelo(
    volume: int,
    processos: int,
    *,
    seed: int = 20261121,
    quantidade_titulos: int = 200,
    quantidade_usuarios: int = 2000,
) -> dict[str, Any]:
    if volume < 1 or processos < 1:
        raise ValueError("volume e processos devem ser positivos")
    tamanho = math.ceil(volume / processos)
    intervalos = [
        (
            inicio,
            min(inicio + tamanho, volume),
            seed,
            quantidade_titulos,
            quantidade_usuarios,
        )
        for inicio in range(0, volume, tamanho)
    ]
    with ProcessPoolExecutor(max_workers=processos) as executor:
        partes = list(executor.map(_processar_intervalo, intervalos))
    return _finalizar(_combinar(partes))


def fingerprint(relatorio: dict[str, Any]) -> str:
    serializado = json.dumps(
        relatorio,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(serializado).hexdigest()
