
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select play_percentage
from spotify_data.gold.device_usage
where play_percentage is null



  
  
      
    ) dbt_internal_test