create view revenue0 (supplier_no, total_revenue) as
	select
		L_SUPPKEY,
		sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT))
	from
		lineitem
	where
		L_SHIPDATE >= date '1994-03-01'
		and L_SHIPDATE < date '1994-03-01' + interval '3 months'
	group by
		L_SUPPKEY;
select
	S_SUPPKEY,
	S_NAME,
	S_ADDRESS,
	S_PHONE,
	total_revenue
from
	supplier,
	revenue0
where
	S_SUPPKEY = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	S_SUPPKEY;
drop view revenue0;
