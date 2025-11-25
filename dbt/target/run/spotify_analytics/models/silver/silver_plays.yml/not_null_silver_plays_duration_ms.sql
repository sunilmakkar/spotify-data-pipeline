
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select duration_ms
from spotify_data.silver.silver_plays
where duration_ms is null



  
  
      
    ) dbt_internal_test