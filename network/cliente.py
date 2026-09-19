import socket

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.protocolo import *

PORTA = 9999
SERVIDOR_IP = "127.0.0.1"


class Cliente:
    def __init__(self, host=SERVIDOR_IP, porta=PORTA):
        self.socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.socket.connect((host, porta))

    def pedir_livro(self, titulo):
        enviar_mensagem(
            self.socket,
            PEDIR,
            titulo
        )

        operacao, resposta = receber_mensagem(self.socket)

        if operacao == ERRO:
            raise RuntimeError(resposta)

        return resposta

    def devolver_livro(self, titulo):
        enviar_mensagem(
            self.socket,
            DEVOLVER,
            titulo
        )

        operacao, resposta = receber_mensagem(self.socket)

        if operacao == ERRO:
            raise RuntimeError(resposta)

        return resposta

    def desconectar(self):
        self.socket.close()


if __name__ == "__main__":
    cliente = Cliente()

    try:
        livro = cliente.pedir_livro("Duna")

        print("Livro recebido:", livro)

        cliente.devolver_livro("Duna")

        print("Livro devolvido.")

    finally:
        cliente.desconectar()