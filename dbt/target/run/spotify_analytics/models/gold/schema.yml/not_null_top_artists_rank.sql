
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rank
from spotify_data.gold.top_artists
where rank is null



  
  
      
    ) dbt_internal_test