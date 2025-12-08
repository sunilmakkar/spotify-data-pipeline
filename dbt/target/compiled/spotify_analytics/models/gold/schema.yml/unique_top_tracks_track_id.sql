
    
    

select
    track_id as unique_field,
    count(*) as n_records

from spotify_data.gold.top_tracks
where track_id is not null
group by track_id
having count(*) > 1


