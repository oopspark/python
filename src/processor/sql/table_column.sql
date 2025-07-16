CREATE OR REPLACE VIEW table_column AS
SELECT
    c.TABLE_NAME,
    c.COLUMN_NAME
FROM
    information_schema.COLUMNS c
JOIN
    information_schema.TABLES t
    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME = t.TABLE_NAME
WHERE
    c.TABLE_SCHEMA = 'bank'
    AND t.TABLE_TYPE = 'BASE TABLE';
