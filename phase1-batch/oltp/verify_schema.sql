-- OLTP schema at a glance: one row per table, PK and FK flagged
SELECT
    t.name AS table_name,
    STRING_AGG(
        c.name +
        CASE
            WHEN ic.column_id IS NOT NULL THEN ' (PK)'
            WHEN fkc.parent_column_id IS NOT NULL
                 THEN ' (FK > ' + OBJECT_NAME(fkc.referenced_object_id) + ')'
            ELSE ''
        END, ', '
    ) WITHIN GROUP (ORDER BY c.column_id) AS columns
FROM sys.tables t
JOIN sys.schemas s   ON s.schema_id = t.schema_id
JOIN sys.columns c   ON c.object_id = t.object_id
LEFT JOIN sys.indexes i
       ON i.object_id = t.object_id AND i.is_primary_key = 1
LEFT JOIN sys.index_columns ic
       ON ic.object_id = t.object_id AND ic.index_id = i.index_id
      AND ic.column_id = c.column_id
LEFT JOIN sys.foreign_key_columns fkc
       ON fkc.parent_object_id = t.object_id
      AND fkc.parent_column_id = c.column_id
WHERE s.name = 'dbo'
GROUP BY t.name
ORDER BY t.name;