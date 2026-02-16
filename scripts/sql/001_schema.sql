CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS process_control (
    process_id UUID PRIMARY KEY,
    source_hash CHAR(64) NOT NULL UNIQUE,
    source_path TEXT NOT NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'ExternalProvider',
    status VARCHAR(20) NOT NULL CHECK (status IN ('Pending', 'Running', 'Completed', 'Failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    message TEXT NULL,
    error_details TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_process_control_status_started
    ON process_control (status, started_at DESC);

CREATE TABLE IF NOT EXISTS staging_transactions (
    staging_id BIGSERIAL PRIMARY KEY,
    process_id UUID NOT NULL,
    source_hash CHAR(64) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    transaction_id CHAR(64) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    cod_fondo INTEGER NOT NULL,
    cod_clase TEXT NULL,
    numero_cuenta_inversion TEXT NULL,
    tipo_identificacion TEXT NULL,
    identificacion INTEGER NOT NULL,
    cod_direccion INTEGER NOT NULL,
    valor NUMERIC(18,2) NOT NULL,
    unidades BIGINT NOT NULL,
    fecha_constitucion TIMESTAMPTZ NULL,
    fecha_vencimiento TIMESTAMPTZ NULL,
    objetivo_inversion BIGINT NOT NULL,
    asesor INTEGER NOT NULL,
    referido TEXT NULL,
    canal_apertura TEXT NULL,
    oficina INTEGER NOT NULL,
    oficina_apertura INTEGER NULL,
    oficina_actual INTEGER NULL,
    bloqueo INTEGER NOT NULL,
    id_causal_bloqueo INTEGER NULL,
    dias_permanencia BIGINT NOT NULL,
    fecha_vto_teorica TIMESTAMPTZ NULL,
    fecha_ultima_cancelacion TIMESTAMPTZ NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_staging_process
    ON staging_transactions (process_id);

CREATE INDEX IF NOT EXISTS ix_staging_tx
    ON staging_transactions (transaction_id, source_system);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_sk BIGSERIAL PRIMARY KEY,
    transaction_id CHAR(64) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    cod_fondo INTEGER NOT NULL,
    cod_clase TEXT NULL,
    numero_cuenta_inversion TEXT NULL,
    tipo_identificacion TEXT NULL,
    identificacion INTEGER NOT NULL,
    cod_direccion INTEGER NOT NULL,
    valor NUMERIC(18,2) NOT NULL,
    unidades BIGINT NOT NULL,
    fecha_constitucion TIMESTAMPTZ NULL,
    fecha_vencimiento TIMESTAMPTZ NULL,
    objetivo_inversion BIGINT NOT NULL,
    asesor INTEGER NOT NULL,
    referido TEXT NULL,
    canal_apertura TEXT NULL,
    oficina INTEGER NOT NULL,
    oficina_apertura INTEGER NULL,
    oficina_actual INTEGER NULL,
    bloqueo INTEGER NOT NULL,
    id_causal_bloqueo INTEGER NULL,
    dias_permanencia BIGINT NOT NULL,
    fecha_vto_teorica TIMESTAMPTZ NULL,
    fecha_ultima_cancelacion TIMESTAMPTZ NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_transactions_transaction UNIQUE (transaction_id, source_system)
);

CREATE INDEX IF NOT EXISTS ix_transactions_fondo_identificacion
    ON transactions (cod_fondo, identificacion);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_process_control_updated_at ON process_control;
CREATE TRIGGER trg_process_control_updated_at
BEFORE UPDATE ON process_control
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_transactions_updated_at ON transactions;
CREATE TRIGGER trg_transactions_updated_at
BEFORE UPDATE ON transactions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
