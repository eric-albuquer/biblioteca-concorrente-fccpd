"""Framing TCP legado com payload JSON do protocolo V2."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import struct
from typing import Any
from uuid import UUID


PEDIR = 1
DEVOLVER = 2
RESPOSTA = 3
ERRO = 4
STATUS = 5

HEADER = struct.Struct("!BI")
MAX_PAYLOAD_BYTES = 64 * 1024
OPERACOES_VALIDAS = {PEDIR, DEVOLVER, RESPOSTA, ERRO, STATUS}


class ErroProtocolo(ValueError):
    def __init__(self, mensagem: str, *, codigo: str = "PROTOCOLO_INVALIDO", fatal=False):
        super().__init__(mensagem)
        self.codigo = codigo
        self.fatal = fatal


def _json_default(valor: Any):
    if isinstance(valor, UUID):
        return str(valor)
    if isinstance(valor, (date, datetime)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return str(valor)
    raise TypeError(f"Objeto {type(valor).__name__} não é serializável em JSON")


def serializar_payload(payload: dict[str, Any]) -> bytes:
    if not isinstance(payload, dict):
        raise ErroProtocolo("O payload V2 deve ser um objeto JSON")
    dados = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    if len(dados) > MAX_PAYLOAD_BYTES:
        raise ErroProtocolo(
            f"Payload excede o limite de {MAX_PAYLOAD_BYTES} bytes",
            codigo="PAYLOAD_MUITO_GRANDE",
            fatal=True,
        )
    return dados


def enviar_mensagem(sock, operacao: int, payload: dict[str, Any]) -> None:
    if operacao not in OPERACOES_VALIDAS:
        raise ErroProtocolo(f"Operação desconhecida: {operacao}")
    dados = serializar_payload(payload)
    sock.sendall(HEADER.pack(operacao, len(dados)) + dados)


def receber_mensagem(sock) -> tuple[int, dict[str, Any]]:
    header = receber_exatamente(sock, HEADER.size)
    operacao, tamanho = HEADER.unpack(header)

    if tamanho > MAX_PAYLOAD_BYTES:
        raise ErroProtocolo(
            f"Payload declarado excede o limite de {MAX_PAYLOAD_BYTES} bytes",
            codigo="PAYLOAD_MUITO_GRANDE",
            fatal=True,
        )

    dados = receber_exatamente(sock, tamanho)
    try:
        payload = json.loads(dados.decode("utf-8"))
    except UnicodeDecodeError as erro:
        raise ErroProtocolo("Payload não é UTF-8 válido") from erro
    except json.JSONDecodeError as erro:
        raise ErroProtocolo("Payload não contém JSON válido") from erro

    if not isinstance(payload, dict):
        raise ErroProtocolo("O payload V2 deve ser um objeto JSON")
    return operacao, payload


def validar_comando(operacao: int, payload: dict[str, Any]) -> dict[str, Any]:
    campos_por_operacao = {
        PEDIR: {"request_id", "usuario_id", "livro_id"},
        DEVOLVER: {"request_id", "usuario_id", "emprestimo_id"},
        STATUS: {"request_id"},
    }
    esperados = campos_por_operacao.get(operacao)
    if esperados is None:
        raise ErroProtocolo(f"Operação de requisição desconhecida: {operacao}")

    recebidos = set(payload)
    ausentes = esperados - recebidos
    extras = recebidos - esperados
    if ausentes:
        raise ErroProtocolo(
            f"Campos obrigatórios ausentes: {', '.join(sorted(ausentes))}"
        )
    if extras:
        raise ErroProtocolo(
            f"Campos não permitidos: {', '.join(sorted(extras))}"
        )

    try:
        request_id = str(UUID(str(payload["request_id"])))
    except (TypeError, ValueError, AttributeError) as erro:
        raise ErroProtocolo("request_id deve ser um UUID válido") from erro

    normalizado: dict[str, Any] = {"request_id": request_id}
    for campo in esperados - {"request_id"}:
        valor = payload[campo]
        if isinstance(valor, bool) or not isinstance(valor, int) or valor <= 0:
            raise ErroProtocolo(f"{campo} deve ser um inteiro positivo")
        normalizado[campo] = valor
    return normalizado


def receber_exatamente(sock, tamanho: int) -> bytes:
    """Lê exatamente ``tamanho`` bytes de um fluxo TCP."""
    dados = bytearray()
    while len(dados) < tamanho:
        bloco = sock.recv(tamanho - len(dados))
        if not bloco:
            raise ConnectionError("Conexão encerrada pelo par remoto.")
        dados.extend(bloco)
    return bytes(dados)
