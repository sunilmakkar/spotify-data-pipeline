
    
    

select
    device_type as unique_field,
    count(*) as n_records

from spotify_data.gold.device_usage
where device_type is not null
group by device_type
having count(*) > 1


