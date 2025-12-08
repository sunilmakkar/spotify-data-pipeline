
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_track_duration_ms
from spotify_data.gold.daily_user_stats
where avg_track_duration_ms is null



  
  
      
    ) dbt_internal_test