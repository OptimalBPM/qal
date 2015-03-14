DELETE "Test"FROM  "Test" AS T1 JOIN (SELECT CSV11 AS "Column1",CSV12 AS "Column2" FROM sysibm.sysdummy1
UNION
SELECT CSV21,CSV22 FROM sysibm.sysdummy1) AS T2 ON ((T2."Column1" = T1."Column1"))
