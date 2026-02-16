ALTER TABLE staging_transactions
    ALTER COLUMN objetivo_inversion TYPE BIGINT USING objetivo_inversion::BIGINT;

ALTER TABLE transactions
    ALTER COLUMN objetivo_inversion TYPE BIGINT USING objetivo_inversion::BIGINT;
