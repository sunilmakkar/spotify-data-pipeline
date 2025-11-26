
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_track_duration_ms
from spotify_data.gold.device_usage
where avg_track_duration_ms is null



  
  
      
    ) dbt_internal_test