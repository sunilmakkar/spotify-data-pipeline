
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_listening_time_ms
from spotify_data.gold.device_usage
where total_listening_time_ms is null



  
  
      
    ) dbt_internal_test