CREATE OR REPLACE VIEW foreign_key_relations AS
SELECT
    kcu.TABLE_SCHEMA AS fk_schema,
    kcu.TABLE_NAME AS fk_table,
    kcu.COLUMN_NAME AS fk_column,
    kcu.REFERENCED_TABLE_SCHEMA AS ref_schema,
    kcu.REFERENCED_TABLE_NAME AS ref_table,
    kcu.REFERENCED_COLUMN_NAME AS ref_column
FROM
    information_schema.KEY_COLUMN_USAGE kcu
WHERE
    kcu.REFERENCED_TABLE_NAME IS NOT NULL
    AND kcu.TABLE_SCHEMA = 'bank';
