# DDIA-
This is a repo of our team for the DDIA labs and projects

# Report
https://docs.google.com/document/d/1SBCpwdJUx35TSRo3BBS8zQviH3uuDd5cgwDPzzWZzvI/edit?tab=t.0

# star schema scripts
SELECT 
    s.s_suppkey, 
    l.l_partkey, 
    o.o_orderkey, 
    c.c_custkey, 
    n.n_nationkey,
    l.l_shipdate, 
    l.l_quantity, 
    l.l_extendedprice, 
    l.l_discount, 
    l.l_tax
FROM Lineitem l
JOIN Supplier s ON l.l_suppkey = s.s_suppkey
JOIN Orders o ON l.l_orderkey = o.o_orderkey
JOIN Customer c ON o.o_custkey = c.c_custkey
JOIN Nation n ON c.c_nationkey = n.n_nationkey;

SELECT 
    n.n_nationkey, 
    n.n_name
FROM Nation n
JOIN Customer c ON n.n_nationkey = c.c_nationkey;

SELECT 
    s.s_suppkey, 
    s.s_name
FROM Supplier s
JOIN Lineitem l ON s.s_suppkey = l.l_suppkey;
