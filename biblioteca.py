import threading
from collections import deque
from datetime import datetime

class Livro:
    def __init__(self, titulo, autor, edicao, genero, ano_publicacao, preco):
        self.titulo = titulo
        self.autor = autor
        self.edicao = edicao
        self.genero = genero
        self.ano_publicacao = ano_publicacao
        self.preco = preco

    def __str__(self):
        return f"""
{{
  titulo: {self.titulo},
  autor: {self.autor},
  edicao: {self.edicao},
  genero: {self.genero},
  ano: {self.ano_publicacao},
  preco: R$ {self.preco:.2f}
}}
        """

class Estoque:
    def __init__(self, livro, capacidade):
        self.livro = livro
        self.disponiveis = capacidade
        self.total = capacidade

        self.fila_espera = deque()
        self.lock = threading.Lock()

    def retirar_exemplar(self):
        """Bloqueia até conseguir um exemplar. Retorna quando o cliente foi atendido."""
        with self.lock:
            if self.disponiveis:
                self.disponiveis -= 1
                return

            pedido = threading.Event()
            self.fila_espera.append(pedido)

        # wait() fora do lock: segurar o lock aqui travaria a biblioteca inteira.
        pedido.wait()

    def devolver_exemplar(self):
        with self.lock:
            if self.fila_espera:
                pedido = self.fila_espera.popleft()
                pedido.set()
            else:
                self.disponiveis += 1

class Biblioteca:
    def __init__(self):
        self.lock = threading.Lock()
        self.estoques = {}

    def cadastrar_livro(self, titulo, autor, edicao, genero, ano_publicacao, preco, capacidade):
        with self.lock:
            if titulo in self.estoques:
                raise ValueError(
                    f"Livro '{titulo}' já existe na biblioteca."
                )

            livro = Livro(titulo, autor, edicao, genero, ano_publicacao, preco)
            self.estoques[titulo] = Estoque(livro, capacidade)

    def emprestar_livro(self, titulo) -> str | None:
        """Bloqueia até ter exemplar. Devolve a ficha do livro, ou None se o título não existe."""
        estoque: Estoque = self.estoques.get(titulo)

        if estoque is None:
            return None

        estoque.retirar_exemplar()

        return str(estoque.livro)

    def devolver_livro(self, titulo) -> bool:
        estoque: Estoque = self.estoques.get(titulo)

        if estoque is None:
            return False

        estoque.devolver_exemplar()

        return True

    def mostrar_estoques(self):
        print("=" * 70)
        print("ESTADO DOS ESTOQUES")
        print("=" * 70)

        for titulo, estoque in self.estoques.items():

            with estoque.lock:
                disponiveis = estoque.disponiveis
                total = estoque.total
                fila = len(estoque.fila_espera)

            print(
                f"{titulo:15} "
                f"Disponíveis: {disponiveis:2} / {total} "
                f"Fila: {fila:3}"
            )

        print()
        print(f"Atualizado: {datetime.now().strftime('%H:%M:%S')}")