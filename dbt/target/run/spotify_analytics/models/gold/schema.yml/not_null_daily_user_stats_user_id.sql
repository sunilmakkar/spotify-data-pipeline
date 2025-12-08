
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from spotify_data.gold.daily_user_stats
where user_id is null



  
  
      
    ) dbt_internal_test