
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select artist_name
from spotify_data.gold.top_artists
where artist_name is null



  
  
      
    ) dbt_internal_test