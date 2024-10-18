SELECT
    sum(p_retailprice) AS total
FROM
    part
WHERE
    p_retailprice > 1000
    AND p_size IN (5, 10, 15, 20)
    AND p_container <> 'JUMBO JAR'
    AND p_mfgr LIKE 'Manufacturer#1%';