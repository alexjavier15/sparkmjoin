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
	partsupp ps1,
	nation,
	region,
(
		select
			min(ps2.PS_SUPPLYCOST) as minv
		from
			partsupp ps2
			part,
			supplier,
			nation,
			region
		where
			P_PARTKEY = PS_PARTKEY
			and S_SUPPKEY = PS_SUPPKEY
			and S_NATIONKEY = N_NATIONKEY
			and N_REGIONKEY = R_REGIONKEY
			and R_NAME = 'MIDDLE EAST'
		order by
			PS_SUPPLYCOST
	) as profit
where
	P_PARTKEY = PS_PARTKEY
	and S_SUPPKEY = PS_SUPPKEY
	and P_SIZE = 26
	and P_TYPE like '%STEEL'
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = 'MIDDLE EAST'
	and ps1.PS_SUPPLYCOST = profit.minv
order by
	S_ACCTBAL desc,
	N_NAME,
	S_NAME,
	P_PARTKEY
limit 100;
