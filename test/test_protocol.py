from __future__ import annotations

import socket
from uuid import uuid4

import pytest

from network.protocolo import (
    HEADER,
    MAX_PAYLOAD_BYTES,
    PEDIR,
    STATUS,
    ErroProtocolo,
    enviar_mensagem,
    receber_mensagem,
    validar_comando,
)


def test_roundtrip_json_preserva_framing_binario():
    origem, destino = socket.socketpair()
    payload = {
        "request_id": str(uuid4()),
        "usuario_id": 1,
        "livro_id": 2,
    }
    try:
        enviar_mensagem(origem, PEDIR, payload)
        assert receber_mensagem(destino) == (PEDIR, payload)
    finally:
        origem.close()
        destino.close()


def test_rejeita_payload_declarado_acima_do_limite_sem_alocar_memoria():
    origem, destino = socket.socketpair()
    try:
        origem.sendall(HEADER.pack(PEDIR, MAX_PAYLOAD_BYTES + 1))
        with pytest.raises(ErroProtocolo, match="excede") as capturado:
            receber_mensagem(destino)
        assert capturado.value.fatal is True
    finally:
        origem.close()
        destino.close()


@pytest.mark.parametrize(
    "dados,mensagem",
    [
        (b"nao-json", "JSON válido"),
        (b"[]", "objeto JSON"),
        (b'"texto"', "objeto JSON"),
    ],
)
def test_rejeita_json_invalido_ou_que_nao_seja_objeto(dados, mensagem):
    origem, destino = socket.socketpair()
    try:
        origem.sendall(HEADER.pack(PEDIR, len(dados)) + dados)
        with pytest.raises(ErroProtocolo, match=mensagem):
            receber_mensagem(destino)
    finally:
        origem.close()
        destino.close()


def test_validacao_normaliza_uuid_e_rejeita_campos_extras():
    request_id = uuid4()
    normalizado = validar_comando(STATUS, {"request_id": request_id})
    assert normalizado == {"request_id": str(request_id)}

    with pytest.raises(ErroProtocolo, match="não permitidos"):
        validar_comando(
            PEDIR,
            {
                "request_id": str(request_id),
                "usuario_id": 1,
                "livro_id": 1,
                "titulo": "Duna",
            },
        )


@pytest.mark.parametrize("valor", [True, 0, -1, "1", 1.5])
def test_validacao_exige_ids_inteiros_positivos(valor):
    with pytest.raises(ErroProtocolo, match="inteiro positivo"):
        validar_comando(
            PEDIR,
            {
                "request_id": str(uuid4()),
                "usuario_id": valor,
                "livro_id": 1,
            },
        )

