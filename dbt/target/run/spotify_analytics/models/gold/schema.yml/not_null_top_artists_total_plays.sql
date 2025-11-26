
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_plays
from spotify_data.gold.top_artists
where total_plays is null



  
  
      
    ) dbt_internal_test