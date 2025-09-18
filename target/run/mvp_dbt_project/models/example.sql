USE [bronze_sales];
    
    

    EXEC('create view "dbo"."example" as -- Example dbt model
select 1 as id, ''hello_dbt'' as message;');


