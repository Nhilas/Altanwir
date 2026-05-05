-- D01_lakehouse_views
SELECT
    s.name AS schema_name,
    o.name AS view_name,
    m.definition
FROM sys.sql_modules m
JOIN sys.objects o ON m.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type = 'V'
  AND s.name IN ('gold', 'silver', 'bronze')
ORDER BY s.name, o.name;

-- D02_audit_views

SELECT
    s.name AS schema_name,
    o.name AS view_name,
    m.definition
FROM sys.sql_modules m
JOIN sys.objects o ON m.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type = 'V'
  AND s.name IN ('steam')
ORDER BY s.name, o.name;
