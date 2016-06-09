select
*
from
	
	nation

where

	N_REGIONKEY  in (
		select
			R_REGIONKEY
		from
		
			region
		where

			R_NAME = 'AMERICA'
	)

limit 100;
