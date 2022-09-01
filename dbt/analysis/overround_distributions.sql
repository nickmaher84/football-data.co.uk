select
	overround::numeric(4,3) overround,
	count(*)
from
    {{ ref('match_1x2') }}
where
	left(bookmaker_code, 2) != 'Bb'
and
	left(bookmaker_code, 3) not in ('Max','Avg')
and
	overround is not null
group by
	overround::numeric(4,3)
order by 1