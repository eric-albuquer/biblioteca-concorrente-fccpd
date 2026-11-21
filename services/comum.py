"""Tipos e funções compartilhados pelos serviços transacionais."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID


class RegraDeNegocioError(RuntimeError):
    """Erro esperado que não deve ser retentado como falha de infraestrutura."""


class EntidadeNaoEncontrada(RegraDeNegocioError):
    pass


class ConflitoIdempotencia(RegraDeNegocioError):
    pass


def normalizar_uuid(request_id: UUID | str) -> UUID:
    try:
        return request_id if isinstance(request_id, UUID) else UUID(str(request_id))
    except (TypeError, ValueError, AttributeError) as erro:
        raise ValueError("request_id deve ser um UUID válido") from erro


def hash_payload(payload: dict[str, Any]) -> str:
    canonico = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonico).hexdigest()


def selecionar_solicitacao(conexao, request_id: UUID) -> dict[str, Any] | None:
    return conexao.execute(
        """
        SELECT request_id, tipo, usuario_id, livro_id, emprestimo_id,
               ordem_fila, status, erro, payload_hash, criado_em,
               atualizado_em, concluido_em
        FROM solicitacoes
        WHERE request_id = %s
        """,
        (request_id,),
    ).fetchone()


def validar_repeticao(
    existente: dict[str, Any], *, tipo: str, payload_hash: str
) -> dict[str, Any]:
    if existente["tipo"] != tipo or existente["payload_hash"].strip() != payload_hash:
        raise ConflitoIdempotencia(
            "request_id já utilizado com tipo ou payload diferente"
        )
    return existente


def bloquear_request_id(conexao, request_id: UUID) -> None:
    """Serializa retransmissões concorrentes somente do mesmo UUID."""
    conexao.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (str(request_id),),
    )

