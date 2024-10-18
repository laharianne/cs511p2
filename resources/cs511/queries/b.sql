SELECT
    s_name,
    SUM(o_totalprice) AS total_order_value
FROM
    supplier,
    nation,
    region,
    orders
WHERE
    s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND s_suppkey = o_custkey
GROUP BY
    s_name
ORDER BY
    total_order_value DESC;