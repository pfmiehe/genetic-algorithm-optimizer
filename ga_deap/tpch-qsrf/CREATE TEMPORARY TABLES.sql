use tpch;
--
-- Table structure for table `orders_temp`
--
DROP TABLE IF EXISTS `orders_temp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `orders_temp` (
  `o_orderkey` bigint(20) NOT NULL,
  `o_custkey` bigint(20) NOT NULL,
  `o_orderstatus` char(1) DEFAULT NULL,
  `o_totalprice` decimal(19,4) DEFAULT NULL,
  `o_orderdate` date DEFAULT NULL,
  `o_orderpriority` char(15) DEFAULT NULL,
  `o_clerk` char(15) DEFAULT NULL,
  `o_shippriority` int(11) DEFAULT NULL,
  `o_comment` varchar(79) DEFAULT NULL,
  PRIMARY KEY (`o_orderkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Table structure for table `lineitem_temp`
--
DROP TABLE IF EXISTS `lineitem_temp`;
CREATE TABLE `lineitem_temp` (
  `l_orderkey` bigint(20) NOT NULL,
  `l_partkey` bigint(20) NOT NULL,
  `l_suppkey` int(11) NOT NULL,
  `l_linenumber` bigint(20) NOT NULL,
  `l_quantity` bigint(20) NOT NULL,
  `l_extendedprice` decimal(19,4) NOT NULL,
  `l_discount` decimal(19,4) NOT NULL,
  `l_tax` decimal(19,4) NOT NULL,
  `l_returnflag` char(1) DEFAULT NULL,
  `l_linestatus` char(1) DEFAULT NULL,
  `l_shipdate` date DEFAULT NULL,
  `l_commitdate` date DEFAULT NULL,
  `l_receiptdate` date DEFAULT NULL,
  `l_shipinstruct` char(25) DEFAULT NULL,
  `l_shipmode` char(10) DEFAULT NULL,
  `l_comment` varchar(44) DEFAULT NULL,
  PRIMARY KEY (`l_orderkey`,`l_linenumber`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Table structure for table `rfdelete`
--
DROP TABLE IF EXISTS `rfdelete`;
CREATE TABLE `rfdelete` (
`rf_orderkey` bigint(20) NOT NULL,
PRIMARY KEY (`rf_orderkey`) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

