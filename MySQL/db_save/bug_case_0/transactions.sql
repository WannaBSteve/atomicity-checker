-- Transaction 1
BEGIN
INSERT IGNORE INTO table_0(c5, c3, c4, c1, c2, id, c0) VALUES (1995914869, "wmtyo", 5.76950019234792, 23.01926780549053, 19.606528135446894, NULL, -68.58307744144253)
SELECT c0, id, c4, c1 FROM table_0 WHERE c4 FOR UPDATE
SELECT c0, c3, c2, c5, c1 FROM table_0 WHERE c4
COMMIT

-- Transaction 2
BEGIN
UPDATE table_0 SET c5=-8, c3='tttcyykffj' WHERE -388
SELECT c5, c3, c1, c0 FROM table_0 WHERE ((c4) | ((c4) != (-856))) - (236)
SELECT c5 FROM table_0 WHERE c1 FOR UPDATE
ROLLBACK

-- Transaction 3
BEGIN
UPDATE table_0 SET c1=-46.23, c0=-42.2, c3='lahxhcerpr' WHERE c0
COMMIT

-- Transaction 4
BEGIN
DELETE FROM table_0 WHERE (c5) IN (358, 273, -213)
INSERT INTO table_0(c2, id, c0, c1, c3, c4) VALUES (98.99475090168636, NULL, 67.55466248502918, 17.629956731803986, "gess", 21.799731126576177)
SELECT c0, c1, c2, c5, id, c4 FROM table_0 WHERE (c0) IN (-20, 649, -393)
UPDATE table_0 SET c0=8.27, id=61, c4=50.4, c1=-97.87, c5=26, c2=5.81 WHERE CONV(((c0) IS NULL), ("otowduwbla"), (CURTIME()))
ROLLBACK

-- Planned Serial
-- Transaction 3: BEGIN
-- Transaction 1: BEGIN
-- Transaction 1: INSERT IGNORE INTO table_0(c5, c3, c4, c1, c2, id, c0) VALUES (1995914869, "wmtyo", 5.76950019234792, 23.01926780549053, 19.606528135446894, NULL, -68.58307744144253)
-- Transaction 3: UPDATE table_0 SET c1=-46.23, c0=-42.2, c3='lahxhcerpr' WHERE c0
-- Transaction 1: SELECT c0, id, c4, c1 FROM table_0 WHERE c4 FOR UPDATE
-- Transaction 1: SELECT c0, c3, c2, c5, c1 FROM table_0 WHERE c4
-- Transaction 3: COMMIT
-- Transaction 2: BEGIN
-- Transaction 4: BEGIN
-- Transaction 4: DELETE FROM table_0 WHERE (c5) IN (358, 273, -213)
-- Transaction 4: INSERT INTO table_0(c2, id, c0, c1, c3, c4) VALUES (98.99475090168636, NULL, 67.55466248502918, 17.629956731803986, "gess", 21.799731126576177)
-- Transaction 2: UPDATE table_0 SET c5=-8, c3='tttcyykffj' WHERE -388
-- Transaction 4: SELECT c0, c1, c2, c5, id, c4 FROM table_0 WHERE (c0) IN (-20, 649, -393)
-- Transaction 1: COMMIT
-- Transaction 4: UPDATE table_0 SET c0=8.27, id=61, c4=50.4, c1=-97.87, c5=26, c2=5.81 WHERE CONV(((c0) IS NULL), ("otowduwbla"), (CURTIME()))
-- Transaction 2: SELECT c5, c3, c1, c0 FROM table_0 WHERE ((c4) | ((c4) != (-856))) - (236)
-- Transaction 2: SELECT c5 FROM table_0 WHERE c1 FOR UPDATE
-- Transaction 4: ROLLBACK
-- Transaction 2: ROLLBACK

-- Actually Executed Serial
-- Transaction 3: BEGIN
-- Transaction 1: BEGIN
-- Transaction 1: INSERT IGNORE INTO table_0(c5, c3, c4, c1, c2, id, c0) VALUES (1995914869, "wmtyo", 5.76950019234792, 23.01926780549053, 19.606528135446894, NULL, -68.58307744144253)
-- Transaction 1: SELECT c0, id, c4, c1 FROM table_0 WHERE c4 FOR UPDATE
-- Transaction 3: COMMIT
-- Transaction 1: SELECT c0, c3, c2, c5, c1 FROM table_0 WHERE c4
-- Transaction 2: BEGIN
-- Transaction 4: BEGIN
-- Transaction 4: DELETE FROM table_0 WHERE (c5) IN (358, 273, -213)
-- Transaction 1: COMMIT
-- Transaction 4: INSERT INTO table_0(c2, id, c0, c1, c3, c4) VALUES (98.99475090168636, NULL, 67.55466248502918, 17.629956731803986, "gess", 21.799731126576177)
-- Transaction 4: SELECT c0, c1, c2, c5, id, c4 FROM table_0 WHERE (c0) IN (-20, 649, -393)
-- Transaction 4: ROLLBACK

-- Cleaned Serial
-- Transaction 3: BEGIN
-- Transaction 1: BEGIN
-- Transaction 1: INSERT IGNORE INTO table_0(c5, c3, c4, c1, c2, id, c0) VALUES (1995914869, "wmtyo", 5.76950019234792, 23.01926780549053, 19.606528135446894, NULL, -68.58307744144253)
-- Transaction 3: UPDATE table_0 SET c1=-46.23, c0=-42.2, c3='lahxhcerpr' WHERE c0
-- Transaction 1: SELECT c0, id, c4, c1 FROM table_0 WHERE c4 FOR UPDATE
-- Transaction 1: SELECT c0, c3, c2, c5, c1 FROM table_0 WHERE c4
-- Transaction 3: COMMIT
-- Transaction 2: BEGIN
-- Transaction 2: UPDATE table_0 SET c5=-8, c3='tttcyykffj' WHERE -388
-- Transaction 1: COMMIT
-- Transaction 2: SELECT c5, c3, c1, c0 FROM table_0 WHERE ((c4) | ((c4) != (-856))) - (236)
-- Transaction 2: SELECT c5 FROM table_0 WHERE c1 FOR UPDATE
-- Transaction 2: ROLLBACK

