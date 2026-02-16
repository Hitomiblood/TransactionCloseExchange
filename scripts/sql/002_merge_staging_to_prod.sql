CREATE OR REPLACE FUNCTION merge_staging_to_transactions(p_process_id UUID)
RETURNS TABLE(inserted_count BIGINT, updated_count BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_updated BIGINT := 0;
BEGIN
    WITH ranked_source AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.transaction_id, s.source_system
                ORDER BY s.ingested_at DESC, s.staging_id DESC
            ) AS rn
        FROM staging_transactions s
        WHERE s.process_id = p_process_id
    ),
    source_dedup AS (
        SELECT *
        FROM ranked_source
        WHERE rn = 1
    ),
    upserted AS (
        INSERT INTO transactions (
            transaction_id,
            source_system,
            payload_hash,
            cod_fondo,
            cod_clase,
            numero_cuenta_inversion,
            tipo_identificacion,
            identificacion,
            cod_direccion,
            valor,
            unidades,
            fecha_constitucion,
            fecha_vencimiento,
            objetivo_inversion,
            asesor,
            referido,
            canal_apertura,
            oficina,
            oficina_apertura,
            oficina_actual,
            bloqueo,
            id_causal_bloqueo,
            dias_permanencia,
            fecha_vto_teorica,
            fecha_ultima_cancelacion
        )
        SELECT
            d.transaction_id,
            d.source_system,
            d.payload_hash,
            d.cod_fondo,
            d.cod_clase,
            d.numero_cuenta_inversion,
            d.tipo_identificacion,
            d.identificacion,
            d.cod_direccion,
            d.valor,
            d.unidades,
            d.fecha_constitucion,
            d.fecha_vencimiento,
            d.objetivo_inversion,
            d.asesor,
            d.referido,
            d.canal_apertura,
            d.oficina,
            d.oficina_apertura,
            d.oficina_actual,
            d.bloqueo,
            d.id_causal_bloqueo,
            d.dias_permanencia,
            d.fecha_vto_teorica,
            d.fecha_ultima_cancelacion
        FROM source_dedup d
        ON CONFLICT (transaction_id, source_system)
        DO UPDATE SET
            payload_hash = EXCLUDED.payload_hash,
            cod_fondo = EXCLUDED.cod_fondo,
            cod_clase = EXCLUDED.cod_clase,
            numero_cuenta_inversion = EXCLUDED.numero_cuenta_inversion,
            tipo_identificacion = EXCLUDED.tipo_identificacion,
            identificacion = EXCLUDED.identificacion,
            cod_direccion = EXCLUDED.cod_direccion,
            valor = EXCLUDED.valor,
            unidades = EXCLUDED.unidades,
            fecha_constitucion = EXCLUDED.fecha_constitucion,
            fecha_vencimiento = EXCLUDED.fecha_vencimiento,
            objetivo_inversion = EXCLUDED.objetivo_inversion,
            asesor = EXCLUDED.asesor,
            referido = EXCLUDED.referido,
            canal_apertura = EXCLUDED.canal_apertura,
            oficina = EXCLUDED.oficina,
            oficina_apertura = EXCLUDED.oficina_apertura,
            oficina_actual = EXCLUDED.oficina_actual,
            bloqueo = EXCLUDED.bloqueo,
            id_causal_bloqueo = EXCLUDED.id_causal_bloqueo,
            dias_permanencia = EXCLUDED.dias_permanencia,
            fecha_vto_teorica = EXCLUDED.fecha_vto_teorica,
            fecha_ultima_cancelacion = EXCLUDED.fecha_ultima_cancelacion,
            updated_at = NOW()
        WHERE transactions.payload_hash <> EXCLUDED.payload_hash
        RETURNING (xmax = 0) AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted),
        COUNT(*) FILTER (WHERE NOT inserted)
    INTO v_inserted, v_updated
    FROM upserted;

    DELETE FROM staging_transactions
    WHERE process_id = p_process_id;

    RETURN QUERY SELECT v_inserted, v_updated;
END;
$$;
