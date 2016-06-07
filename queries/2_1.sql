select
	S_ACCTBAL,
	S_NAME,
	N_NAME,
	P_PARTKEY,
	P_MFGR,
	S_ADDRESS,
	S_PHONE,
	S_COMMENT
from
	part,
	supplier,
	partsupp,
	nation,
	region,
	(
		select
			P_PARTKEY as P_PARTKEY1, min(PS_SUPPLYCOST) as MIN_COST
		from	
			part,
			partsupp,
			supplier,
			nation,
			region
		where
			P_PARTKEY = PS_PARTKEY
			and P_SIZE = 38
			and P_TYPE like '%COPPER'
			and S_SUPPKEY = PS_SUPPKEY
			and S_NATIONKEY = N_NATIONKEY
			and N_REGIONKEY = R_REGIONKEY
			and R_NAME = 'AMERICA'
		group by P_PARTKEY
	) tmp1
where
	P_PARTKEY = PS_PARTKEY1
	and P_PARTKEY = PKEY
	and S_SUPPKEY = PS_SUPPKEY
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = 'AMERICA'
	and PS_SUPPLYCOST = MIN_COST
order by
	S_ACCTBAL desc,
	N_NAME,
	S_NAME,
	P_PARTKEY
limit 100;
