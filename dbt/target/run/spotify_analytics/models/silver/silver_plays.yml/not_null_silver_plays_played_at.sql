
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select played_at
from spotify_data.silver.silver_plays
where played_at is null



  
  
      
    ) dbt_internal_test