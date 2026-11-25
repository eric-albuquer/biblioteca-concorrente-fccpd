CREATE TABLE IF NOT EXISTS books (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL UNIQUE,
    author TEXT NOT NULL,
    edition INTEGER NOT NULL CHECK (edition > 0),
    category TEXT NOT NULL CHECK (
        category IN (
            'ficcao-cientifica', 'romance', 'aventura',
            'acao', 'suspense', 'terror'
        )
    ),
    publication_year INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    total_copies INTEGER NOT NULL CHECK (total_copies > 0),
    available_copies INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT available_copies_in_range CHECK (
        available_copies >= 0 AND available_copies <= total_copies
    )
);

CREATE TABLE IF NOT EXISTS loans (
    id UUID PRIMARY KEY,
    client_id TEXT NOT NULL,
    book_id UUID NOT NULL REFERENCES books(id),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETURNED')),
    borrowed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    returned_at TIMESTAMPTZ,
    borrow_request_id UUID NOT NULL UNIQUE,
    return_request_id UUID UNIQUE,
    CONSTRAINT returned_state_is_consistent CHECK (
        (status = 'ACTIVE' AND returned_at IS NULL AND return_request_id IS NULL)
        OR
        (status = 'RETURNED' AND returned_at IS NOT NULL AND return_request_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_loan_per_client_book
    ON loans (client_id, book_id)
    WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS waitlist (
    id BIGSERIAL PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE,
    client_id TEXT NOT NULL,
    reply_routing_key TEXT NOT NULL,
    book_id UUID NOT NULL REFERENCES books(id),
    status TEXT NOT NULL CHECK (status IN ('WAITING', 'GRANTED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    granted_at TIMESTAMPTZ,
    granted_loan_id UUID REFERENCES loans(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_waiting_request_per_client_book
    ON waitlist (client_id, book_id)
    WHERE status = 'WAITING';

CREATE TABLE IF NOT EXISTS processed_requests (
    request_id UUID PRIMARY KEY,
    client_id TEXT NOT NULL,
    action TEXT NOT NULL,
    outbound_messages JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO books (
    id, title, author, edition, category, publication_year,
    price, total_copies, available_copies
) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Duna', 'Frank Herbert', 1, 'ficcao-cientifica', 1965, 59.90, 2, 2),
    ('22222222-2222-2222-2222-222222222222', 'Orgulho e Preconceito', 'Jane Austen', 1, 'romance', 1813, 39.90, 2, 2),
    ('33333333-3333-3333-3333-333333333333', 'A Ilha do Tesouro', 'Robert Louis Stevenson', 1, 'aventura', 1883, 34.90, 2, 2),
    ('44444444-4444-4444-4444-444444444444', 'Jogos Vorazes', 'Suzanne Collins', 1, 'acao', 2008, 44.90, 2, 2),
    ('55555555-5555-5555-5555-555555555555', 'Garota Exemplar', 'Gillian Flynn', 1, 'suspense', 2012, 49.90, 2, 2),
    ('66666666-6666-6666-6666-666666666666', 'It', 'Stephen King', 1, 'terror', 1986, 69.90, 2, 2)
ON CONFLICT (id) DO NOTHING;
