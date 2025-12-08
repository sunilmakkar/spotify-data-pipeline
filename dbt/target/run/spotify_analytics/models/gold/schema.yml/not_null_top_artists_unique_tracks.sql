
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_tracks
from spotify_data.gold.top_artists
where unique_tracks is null



  
  
      
    ) dbt_internal_test