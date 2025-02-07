-- Transaction 1
BEGIN;
INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'irutA', 'TWHAK', 'BgPBG', 68);
INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'wZjQw', 'UvGES', 'zBqBR', 71);
INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'nQFnI', 'aMWZu', 'dCKuV', 54);
SELECT * FROM table_0 WHERE (col_0 = 'nfuhz') FOR UPDATE;
COMMIT;

-- Transaction 2
BEGIN
SELECT * FROM table_0 WHERE id >= 2 AND id <= 2 FOR UPDATE
SELECT id, col_0, col_689 FROM table_0 WHERE id >= 7 AND id <= 7 FOR UPDATE
SELECT id, col_0, col_2, col_689 FROM table_0 WHERE (col_0 = 'lopiy') AND (col_2 = 'hpfcg') AND (col_1 = 'nrnuu') FOR UPDATE
COMMIT;

-- Planned Serial
-- Transaction 2: SELECT * FROM table_0 WHERE id >= 2 AND id <= 2 FOR UPDATE
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'irutA', 'TWHAK', 'BgPBG', 68)
-- Transaction 2: SELECT id, col_0, col_689 FROM table_0 WHERE id >= 7 AND id <= 7 FOR UPDATE
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'wZjQw', 'UvGES', 'zBqBR', 71)
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'nQFnI', 'aMWZu', 'dCKuV', 54)
-- Transaction 2: SELECT id, col_0, col_2, col_689 FROM table_0 WHERE (col_0 = 'lopiy') AND (col_2 = 'hpfcg') AND (col_1 = 'nrnuu') FOR UPDATE
-- Transaction 1: SELECT * FROM table_0 WHERE (col_0 = 'nfuhz') FOR UPDATE
-- Transaction 1: COMMIT
-- Transaction 2: COMMIT

-- Actually Executed Serial
-- Transaction 2: SELECT * FROM table_0 WHERE id >= 2 AND id <= 2 FOR UPDATE
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'irutA', 'TWHAK', 'BgPBG', 68)
-- Transaction 2: SELECT id, col_0, col_689 FROM table_0 WHERE id >= 7 AND id <= 7 FOR UPDATE
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'wZjQw', 'UvGES', 'zBqBR', 71)
-- Transaction 1: INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'nQFnI', 'aMWZu', 'dCKuV', 54)
-- Transaction 2: -- Lock blocked: SELECT id, col_0, col_2, col_689 FROM table_0 WHERE (col_0 = 'lopiy') AND (col_2 = 'hpfcg') AND (col_1 = 'nrnuu') FOR UPDATE
-- Transaction 1: -- Lock blocked: SELECT * FROM table_0 WHERE (col_0 = 'nfuhz') FOR UPDATE
-- Transaction 1: COMMIT
-- Transaction 2: COMMIT