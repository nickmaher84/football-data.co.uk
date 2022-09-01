with
odds as (
	select
		unnest(array['H','D','A']) as side,
		unnest(array[home_odds,draw_odds,away_odds]) as odds
	from
        {{ ref('match_1x2') }}
	where
	    left(bookmaker_code, 2) != 'Bb'
	and
		left(bookmaker_code, 3) not in ('Max','Avg')
)
select
	odds,
	count(case side when 'H' then 1 end) home,
	count(case side when 'D' then 1 end) draw,
	count(case side when 'A' then 1 end) away,
	count(*) as total
from
	odds
where
	odds is not null
group by
	odds
order by 1