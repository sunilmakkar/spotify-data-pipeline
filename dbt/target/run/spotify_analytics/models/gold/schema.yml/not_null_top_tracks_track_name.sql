
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select track_name
from spotify_data.gold.top_tracks
where track_name is null



  
  
      
    ) dbt_internal_test