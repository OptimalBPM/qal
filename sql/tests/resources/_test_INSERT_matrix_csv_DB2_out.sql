INSERT INTO "Test" ("Column1", "Column2")(SELECT 'Matrix11' AS "Column1",'Matrix12' AS "Column2" FROM sysibm.sysdummy1
UNION
SELECT 'Matrix21','Matrix22' FROM sysibm.sysdummy1)
UNION
(SELECT CSV11 AS "Column1",CSV12 AS "Column2" FROM sysibm.sysdummy1
UNION
SELECT CSV21,CSV22 FROM sysibm.sysdummy1)
