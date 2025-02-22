package com.spark.ApacheSpark;

import java.util.List;
import java.util.Map;

public interface Repository extends JpaRepository<Supplier, Integer> {

    @Query(
            "select \n" +
                    "\tn_name, \n" +
                    "\ts_name,\n" +
                    "\tsum(l_quantity) as sum_qty,\n" +
                    "\tsum(l_extendedprice) as sum_base_price,\n" +
                    "\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
                    "\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
                    "\tavg(l_quantity) as avg_qty,\n" +
                    "\tavg(l_extendedprice) as avg_price,\n" +
                    "\tavg(l_discount) as avg_disc ,\n" +
                    "\tcount(*) as count_order\n" +
                    "from\n" +
                    "\tLineitem,\n" +
                    "\tOrders,\n" +
                    "\tCustomer,\n" +
                    "\tNation,\n" +
                    "\tPartsupp,\n" +
                    "\tSupplier\n" +
                    "where\n" +
                    "\tl_shipdate <= date'1998-12-01' -interval '90' day AND\n" +
                    "\tl_orderkey = o_orderkey AND\n" +
                    "\to_custkey = c_custkey AND \n" +
                    "\tc_nationkey = n_nationkey AND\n" +
                    "\tl_partkey = ps_partkey AND\n" +
                    "\tl_suppkey = ps_suppkey AND\n" +
                    "\tps_suppkey = s_suppkey\n" +
                    "group by \n" +
                    "\tn_name ,  \n" +
                    "\ts_name ;\n"
    )
    List<Map<String, Object>> runQuery();

}

