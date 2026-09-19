import struct

PEDIR = 1
DEVOLVER = 2
RESPOSTA = 3
ERRO = 4

HEADER = struct.Struct("!BI")

def enviar_mensagem(sock, operacao, payload=""):
    dados = payload.encode("utf-8")

    header = HEADER.pack(
        operacao,
        len(dados)
    )

    sock.sendall(header + dados)


def receber_mensagem(sock):
    header = receber_exatamente(sock, HEADER.size)

    operacao, tamanho = HEADER.unpack(header)

    payload = receber_exatamente(sock, tamanho)

    return operacao, payload.decode("utf-8")


def receber_exatamente(sock, tamanho):
    """Lê exatamente `tamanho` bytes. TCP é um fluxo, então um recv() pode
    devolver menos bytes do que o pedido e o laço é obrigatório."""
    dados = bytearray()

    while len(dados) < tamanho:
        bloco = sock.recv(tamanho - len(dados))

        if not bloco:
            raise ConnectionError("Conexão encerrada pelo cliente.")

        dados.extend(bloco)

    return bytes(dados)