select
	cntrycode,
	count(*) as numcust,
	sum(C_ACCTBAL) as totacctbal
from
	(
		select
			substring(C_PHONE from 1 for 2) as cntrycode,
			C_ACCTBAL
		from
			customer
		where
			substring(C_PHONE from 1 for 2) in
				('34', '20', '23', '44', '31', '32', '30')
			and C_ACCTBAL > (
				select
					avg(C_ACCTBAL)
				from
					customer
				where
					C_ACCTBAL > 0.00
					and substring(C_PHONE from 1 for 2) in
						('34', '20', '23', '44', '31', '32', '30')
			)
			and not exists (
				select
					*
				from
					orders
				where
					O_CUSTKEY = C_CUSTKEY
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
