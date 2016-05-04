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
	region
where
	P_PARTKEY = PS_PARTKEY
	and S_SUPPKEY = PS_SUPPKEY
	and P_SIZE = 39
	and P_TYPE like '%TIN'
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = 'ASIA'
	and PS_SUPPLYCOST = (
		select
			min(PS_SUPPLYCOST)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			P_PARTKEY = PS_PARTKEY
			and S_SUPPKEY = PS_SUPPKEY
			and S_NATIONKEY = N_NATIONKEY
			and N_REGIONKEY = R_REGIONKEY
			and R_NAME = 'ASIA'
	)
order by
	S_ACCTBAL desc,
	N_NAME,
	S_NAME,
	P_PARTKEY
limit 100;
