BEGIN;

INSERT INTO usuarios (id, nome) VALUES
    (1, 'Amanda Luz Chaves'),
    (2, 'Eric Gonçalves Albuquerque'),
    (3, 'Gabriel Aniceto de Sousa Alencar Barros'),
    (4, 'Mircio Ferreira Dos Santos Neto')
ON CONFLICT (id) DO NOTHING;

INSERT INTO livros (
    id, titulo, autor, edicao, genero, ano_publicacao, preco,
    capacidade, disponiveis
) VALUES
    (1, 'Duna', 'Frank Herbert', 1, 'Ficção Científica', 1965, 59.90, 3, 3),
    (2, '1984', 'George Orwell', 2, 'Distopia', 1949, 45.00, 2, 2),
    (3, 'O Hobbit', 'J. R. R. Tolkien', 4, 'Fantasia', 1937, 39.90, 4, 4)
ON CONFLICT (id) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('usuarios', 'id'),
    GREATEST((SELECT COALESCE(MAX(id), 1) FROM usuarios), 1)
);
SELECT setval(
    pg_get_serial_sequence('livros', 'id'),
    GREATEST((SELECT COALESCE(MAX(id), 1) FROM livros), 1)
);

COMMIT;

