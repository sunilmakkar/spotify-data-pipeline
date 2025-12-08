
    
    

select
    rank as unique_field,
    count(*) as n_records

from spotify_data.gold.top_artists
where rank is not null
group by rank
having count(*) > 1


