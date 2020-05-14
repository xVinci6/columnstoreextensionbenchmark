CREATE TABLE nation
(
    n_nationkey  INTEGER not null CONSTRAINT nation_pkey PRIMARY KEY,
    n_name       CHAR(25) not null,
    n_regionkey  INTEGER not null,
    n_comment    VARCHAR(152),
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE region
(
    r_regionkey  INTEGER not null CONSTRAINT region_pkey PRIMARY KEY,
    r_name       CHAR(25) not null,
    r_comment    VARCHAR(152),
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE part
(
    p_partkey     BIGINT not null CONSTRAINT part_kpey PRIMARY KEY,
    p_name        VARCHAR(55) not null,
    p_mfgr        CHAR(25) not null,
    p_brand       CHAR(10) not null,
    p_type        VARCHAR(25) not null,
    p_size        INTEGER not null,
    p_container   CHAR(10) not null,
    p_retailprice DOUBLE PRECISION not null,
    p_comment     VARCHAR(23) not null,
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE supplier
(
    s_suppkey     BIGINT not null CONSTRAINT supplier_pkey PRIMARY KEY,
    s_name        CHAR(25) not null,
    s_address     VARCHAR(40) not null,
    s_nationkey   INTEGER not null,
    s_phone       CHAR(15) not null,
    s_acctbal     DOUBLE PRECISION not null,
    s_comment     VARCHAR(101) not null,
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DOUBLE PRECISION  not null,
    ps_comment     VARCHAR(199) not null,
    CONSTRAINT partsupp_pkey PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE customer
(
    c_custkey     BIGINT not null CONSTRAINT customer_pkey PRIMARY KEY,
    c_name        VARCHAR(25) not null,
    c_address     VARCHAR(40) not null,
    c_nationkey   INTEGER not null,
    c_phone       CHAR(15) not null,
    c_acctbal     DOUBLE PRECISION   not null,
    c_mktsegment  CHAR(10) not null,
    c_comment     VARCHAR(117) not null,
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE orders
(
    o_orderkey       BIGINT not null CONSTRAINT orders_pkey PRIMARY KEY,
    o_custkey        BIGINT not null,
    o_orderstatus    CHAR(1) not null,
    o_totalprice     DOUBLE PRECISION not null,
    o_orderdate      DATE not null,
    o_orderpriority  CHAR(15) not null,
    o_clerk          CHAR(15) not null,
    o_shippriority   INTEGER not null,
    o_comment        VARCHAR(79) not null,
    INDEX ci CLUSTERED COLUMNSTORE
);

CREATE TABLE lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DOUBLE PRECISION not null,
    l_extendedprice  DOUBLE PRECISION not null,
    l_discount    DOUBLE PRECISION not null,
    l_tax         DOUBLE PRECISION not null,
    l_returnflag  CHAR(1) not null,
    l_linestatus  CHAR(1) not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct CHAR(25) not null,
    l_shipmode     CHAR(10) not null,
    l_comment      VARCHAR(44) not null,
    CONSTRAINT lineitem_pkey PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),
    INDEX ci CLUSTERED COLUMNSTORE
);

ALTER TABLE PARTSUPP
ADD CONSTRAINT partsupp_part_fkey
   FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY);

ALTER TABLE PARTSUPP
ADD CONSTRAINT partsupp_supplier_fkey
   FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY);

ALTER TABLE CUSTOMER
ADD CONSTRAINT customer_nation_fkey
   FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY);

ALTER TABLE ORDERS
ADD CONSTRAINT orders_customer_fkey
   FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY);

ALTER TABLE LINEITEM
ADD CONSTRAINT lineitem_orders_fkey
   FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY);

ALTER TABLE LINEITEM
ADD CONSTRAINT lineitem_parts_fkey
   FOREIGN KEY (L_PARTKEY) REFERENCES PART(P_PARTKEY);

ALTER TABLE LINEITEM
ADD CONSTRAINT lineitem_suppliers_fkey
   FOREIGN KEY (L_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY);

ALTER TABLE LINEITEM
ADD CONSTRAINT lineitem_partsupp_fkey
   FOREIGN KEY (L_PARTKEY,L_SUPPKEY)
    REFERENCES PARTSUPP(PS_PARTKEY,PS_SUPPKEY);

ALTER TABLE NATION
ADD CONSTRAINT nation_region_fkey
   FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY);