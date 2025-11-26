
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    rank as unique_field,
    count(*) as n_records

from spotify_data.gold.top_artists
where rank is not null
group by rank
having count(*) > 1



  
  
      
    ) dbt_internal_test