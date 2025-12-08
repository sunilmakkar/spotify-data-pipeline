
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select first_played_at
from spotify_data.gold.top_tracks
where first_played_at is null



  
  
      
    ) dbt_internal_test