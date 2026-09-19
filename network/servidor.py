import socket
import threading
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from biblioteca import Biblioteca
from network.protocolo import *

HOST = "0.0.0.0"
PORTA = 9999

class Servidor:
    def __init__(self):
        self.biblioteca = Biblioteca()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def iniciar(self):
        self.socket.bind((HOST, PORTA))
        self.socket.listen()

        print(f"Servidor ouvindo em {HOST}:{PORTA}")

        threading.Thread(
            target=self.monitorar_estoques,
            daemon=True
        ).start() # Thread de monitoramento

        while True:
            cliente, endereco = self.socket.accept()

            print(f"Cliente conectado: {endereco}")

            threading.Thread(
                target=self.atender_cliente, args=(cliente, endereco), daemon=True
            ).start() # Thread que lida com um cliente

    def atender_cliente(self, cliente, endereco):
        try:
            while True:
                mensagem = receber_mensagem(cliente)

                operacao, titulo = mensagem

                if operacao == PEDIR:
                    self.tratar_pedido(cliente, titulo)

                elif operacao == DEVOLVER:
                    self.tratar_devolucao(cliente, titulo)

                else:
                    enviar_mensagem(cliente, ERRO, "Operação desconhecida")

        except ConnectionError:
            pass

        except Exception as erro:
            print(f"Erro com cliente {endereco}: {erro}")

        finally:
            cliente.close()

    def tratar_pedido(self, cliente, titulo):
        # Esta chamada bloqueia se o estoque estiver zerado, mas só prende
        # a thread deste cliente. As outras seguem sendo atendidas.
        ficha = self.biblioteca.emprestar_livro(titulo)

        if ficha is None:
            enviar_mensagem(cliente, ERRO, f"Livro '{titulo}' não existe.")
            return

        enviar_mensagem(cliente, RESPOSTA, ficha)

    def tratar_devolucao(self, cliente, titulo):
        devolvido = self.biblioteca.devolver_livro(titulo)

        if not devolvido:
            enviar_mensagem(cliente, ERRO, f"Livro '{titulo}' não existe.")
            return

        enviar_mensagem(cliente, RESPOSTA, "Livro devolvido.")

    def monitorar_estoques(self):
        while True:
            os.system("clear")  # Linux/macOS
            self.biblioteca.mostrar_estoques()
            time.sleep(1)


if __name__ == "__main__":
    servidor = Servidor()

    servidor.biblioteca.cadastrar_livro("Duna", "Mircio", 1, "Ficção Científica", 1965, 59.90, 10)
    servidor.biblioteca.cadastrar_livro("1984", "Amanda", 2, "Distopia", 1949, 45.00, 13)
    servidor.biblioteca.cadastrar_livro("O Hobbit", "Tolkien", 4, "Fantasia", 1937, 39.90, 8)

    servidor.iniciar()
