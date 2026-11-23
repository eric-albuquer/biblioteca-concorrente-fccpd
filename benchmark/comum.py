"""Métricas, ambiente e amostragem de recursos dos benchmarks."""

from __future__ import annotations

import csv
import ctypes
from ctypes import wintypes
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import threading
import time
from typing import Any, Iterable


def percentil(valores: Iterable[float], percentual: float) -> float:
    """Calcula percentil pelo método nearest-rank, sem interpolação."""
    ordenados = sorted(valores)
    if not ordenados:
        return 0.0
    if not 0 <= percentual <= 100:
        raise ValueError("percentual deve estar entre 0 e 100")
    indice = max(0, math.ceil(percentual / 100 * len(ordenados)) - 1)
    return float(ordenados[indice])


def resumir_latencias(valores_ms: list[float], prefixo: str) -> dict[str, float]:
    if not valores_ms:
        return {
            f"{prefixo}_media_ms": 0.0,
            f"{prefixo}_p50_ms": 0.0,
            f"{prefixo}_p95_ms": 0.0,
            f"{prefixo}_p99_ms": 0.0,
        }
    return {
        f"{prefixo}_media_ms": statistics.fmean(valores_ms),
        f"{prefixo}_p50_ms": percentil(valores_ms, 50),
        f"{prefixo}_p95_ms": percentil(valores_ms, 95),
        f"{prefixo}_p99_ms": percentil(valores_ms, 99),
    }


def escrever_csv(caminho: str | Path, linhas: list[dict[str, Any]]) -> None:
    destino = Path(caminho)
    destino.parent.mkdir(parents=True, exist_ok=True)
    if not linhas:
        raise ValueError(f"Nenhuma linha para escrever em {destino}")
    campos = list(linhas[0])
    with destino.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(linhas)


def resumir_repeticoes(
    linhas: list[dict[str, Any]],
    chaves: tuple[str, ...],
    metricas: tuple[str, ...],
) -> list[dict[str, Any]]:
    grupos: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for linha in linhas:
        chave = tuple(linha[campo] for campo in chaves)
        grupos.setdefault(chave, []).append(linha)

    resumo: list[dict[str, Any]] = []
    for chave, repeticoes in sorted(grupos.items(), key=lambda item: item[0]):
        linha_resumo = dict(zip(chaves, chave))
        linha_resumo["repeticoes"] = len(repeticoes)
        for metrica in metricas:
            valores = [float(linha[metrica]) for linha in repeticoes]
            linha_resumo[f"{metrica}_media"] = statistics.fmean(valores)
            linha_resumo[f"{metrica}_mediana"] = statistics.median(valores)
            linha_resumo[f"{metrica}_desvio"] = statistics.pstdev(valores)
            linha_resumo[f"{metrica}_min"] = min(valores)
            linha_resumo[f"{metrica}_max"] = max(valores)
        resumo.append(linha_resumo)
    return resumo


def _memoria_total_bytes() -> int | None:
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("dwLength", wintypes.DWORD),
                ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        estado = MemoryStatus()
        estado.dwLength = ctypes.sizeof(estado)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(estado)):
            return int(estado.ullTotalPhys)
        return None
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None


def _versao_comando(comando: list[str]) -> str | None:
    try:
        resultado = subprocess.run(
            comando,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    texto = (resultado.stdout or resultado.stderr).strip()
    return texto.splitlines()[0] if texto else None


def ambiente_base() -> dict[str, Any]:
    memoria = _memoria_total_bytes()
    return {
        "coletado_em_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sistema": platform.platform(),
        "maquina": platform.machine(),
        "processador": platform.processor() or "não informado pelo SO",
        "cpu_logicas": os.cpu_count(),
        "ram_bytes": memoria,
        "ram_gib": round(memoria / 1024**3, 2) if memoria else None,
        "python": sys.version.replace("\n", " "),
        "host_informado": os.getenv("BENCHMARK_HOST"),
        "docker": os.getenv("BENCHMARK_DOCKER_VERSION")
        or _versao_comando(["docker", "--version"]),
        "docker_compose": os.getenv("BENCHMARK_COMPOSE_VERSION")
        or _versao_comando(["docker", "compose", "version"]),
        "limites_compose": "sem limites explícitos de CPU/memória",
    }


def escrever_json(caminho: str | Path, conteudo: dict[str, Any]) -> None:
    destino = Path(caminho)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(
        json.dumps(conteudo, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _metricas_windows(pid: int) -> tuple[float, int] | None:
    process_query = 0x1000
    process_vm_read = 0x0010
    kernel32 = ctypes.windll.kernel32
    psapi = ctypes.windll.psapi
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(process_query | process_vm_read, False, pid)
    if not handle:
        return None

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    criacao = wintypes.FILETIME()
    saida = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    usuario = wintypes.FILETIME()
    memoria = ProcessMemoryCounters()
    memoria.cb = ctypes.sizeof(memoria)
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
    try:
        tempos_ok = kernel32.GetProcessTimes(
            handle,
            ctypes.byref(criacao),
            ctypes.byref(saida),
            ctypes.byref(kernel),
            ctypes.byref(usuario),
        )
        memoria_ok = psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(memoria),
            memoria.cb,
        )
        if not tempos_ok or not memoria_ok:
            return None
        kernel_ticks = (kernel.dwHighDateTime << 32) | kernel.dwLowDateTime
        usuario_ticks = (usuario.dwHighDateTime << 32) | usuario.dwLowDateTime
        return (kernel_ticks + usuario_ticks) / 10_000_000, memoria.WorkingSetSize
    finally:
        kernel32.CloseHandle(handle)


def _metricas_proc(pid: int) -> tuple[float, int] | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        campos = stat[stat.rfind(")") + 2 :].split()
        ticks = int(campos[11]) + int(campos[12])
        rss_paginas = int(campos[21])
        return (
            ticks / os.sysconf("SC_CLK_TCK"),
            rss_paginas * os.sysconf("SC_PAGE_SIZE"),
        )
    except (OSError, ValueError, IndexError):
        return None


def metricas_processo(pid: int) -> tuple[float, int] | None:
    return _metricas_windows(pid) if os.name == "nt" else _metricas_proc(pid)


def _descendentes_windows(raizes: list[int]) -> set[int]:
    snapshot_processos = 0x00000002
    kernel32 = ctypes.windll.kernel32

    class ProcessEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessEntry),
    ]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessEntry),
    ]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel32.CreateToolhelp32Snapshot(snapshot_processos, 0)
    handle_invalido = ctypes.c_void_p(-1).value
    if not snapshot or snapshot == handle_invalido:
        return set()
    filhos: dict[int, list[int]] = {}
    entrada = ProcessEntry()
    entrada.dwSize = ctypes.sizeof(entrada)
    try:
        sucesso = kernel32.Process32FirstW(snapshot, ctypes.byref(entrada))
        while sucesso:
            filhos.setdefault(int(entrada.th32ParentProcessID), []).append(
                int(entrada.th32ProcessID)
            )
            sucesso = kernel32.Process32NextW(snapshot, ctypes.byref(entrada))
    finally:
        kernel32.CloseHandle(snapshot)
    encontrados: set[int] = set()
    pendentes = list(raizes)
    while pendentes:
        pai = pendentes.pop()
        for filho in filhos.get(pai, []):
            if filho not in encontrados:
                encontrados.add(filho)
                pendentes.append(filho)
    return encontrados


def _descendentes_proc(raizes: list[int]) -> set[int]:
    filhos: dict[int, list[int]] = {}
    for entrada in Path("/proc").iterdir():
        if not entrada.name.isdigit():
            continue
        try:
            stat = (entrada / "stat").read_text(encoding="utf-8")
            campos = stat[stat.rfind(")") + 2 :].split()
            filhos.setdefault(int(campos[1]), []).append(int(entrada.name))
        except (OSError, ValueError, IndexError):
            continue
    encontrados: set[int] = set()
    pendentes = list(raizes)
    while pendentes:
        pai = pendentes.pop()
        for filho in filhos.get(pai, []):
            if filho not in encontrados:
                encontrados.add(filho)
                pendentes.append(filho)
    return encontrados


def descendentes_processos(raizes: list[int]) -> set[int]:
    return (
        _descendentes_windows(raizes)
        if os.name == "nt"
        else _descendentes_proc(raizes)
    )


class AmostradorProcessos:
    """Amostra CPU acumulada e pico de RSS agregado sem dependência externa."""

    def __init__(self, pids: list[int], intervalo: float = 0.05):
        self.pids = pids
        self.intervalo = intervalo
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None
        self._inicio_cpu = 0.0
        self._ultimo_cpu = 0.0
        self._pico_rss = 0
        self._inicio = 0.0

    def iniciar(self) -> None:
        pids = set(self.pids) | descendentes_processos(self.pids)
        metricas = [metrica for pid in pids if (metrica := metricas_processo(pid))]
        self._inicio_cpu = sum(item[0] for item in metricas)
        self._ultimo_cpu = self._inicio_cpu
        self._pico_rss = sum(item[1] for item in metricas)
        self._inicio = time.perf_counter()
        self._thread = threading.Thread(target=self._executar, daemon=True)
        self._thread.start()

    def _executar(self) -> None:
        while not self._parar.wait(self.intervalo):
            self._amostrar()

    def _amostrar(self) -> None:
        pids = set(self.pids) | descendentes_processos(self.pids)
        metricas = [metrica for pid in pids if (metrica := metricas_processo(pid))]
        if metricas:
            self._ultimo_cpu = sum(item[0] for item in metricas)
            self._pico_rss = max(self._pico_rss, sum(item[1] for item in metricas))

    def encerrar(self) -> dict[str, float]:
        self._parar.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._amostrar()
        duracao = max(time.perf_counter() - self._inicio, 1e-9)
        cpu = max(self._ultimo_cpu - self._inicio_cpu, 0.0)
        return {
            "workers_cpu_s": cpu,
            "workers_cpu_pct": cpu / duracao * 100,
            "workers_rss_pico_mb": self._pico_rss / 1024**2,
        }

