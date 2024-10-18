SELECT
	c_custkey,
	c_name,
	c_acctbal,
	sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
	customer,
	orders,
	lineitem
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate >= date '1993-10-01'
	AND o_orderdate < date '1993-10-01' + interval '3' month
GROUP BY
	c_custkey,
	c_name,
	c_acctbal
ORDER BY
	revenue DESC;