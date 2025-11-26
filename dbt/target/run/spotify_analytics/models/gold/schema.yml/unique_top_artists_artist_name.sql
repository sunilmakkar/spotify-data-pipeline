
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    artist_name as unique_field,
    count(*) as n_records

from spotify_data.gold.top_artists
where artist_name is not null
group by artist_name
having count(*) > 1



  
  
      
    ) dbt_internal_test