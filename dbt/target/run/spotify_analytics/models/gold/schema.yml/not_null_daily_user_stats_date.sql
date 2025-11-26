
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from spotify_data.gold.daily_user_stats
where date is null



  
  
      
    ) dbt_internal_test