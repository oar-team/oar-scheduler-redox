PRAGMA foreign_keys = OFF;

BEGIN TRANSACTION;

SELECT 'DELETE FROM "' || name || '";'
FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%';

COMMIT;

PRAGMA foreign_keys = ON;
