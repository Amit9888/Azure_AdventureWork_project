
CREATE  View  Gold.calender  as 
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Calendar/',FORMAT='PARQUET') as query1




CREATE View Gold.customers  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Customers/',FORMAT='PARQUET') as query1

)

CREATE View Gold.Product_Subcategories  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/',FORMAT='PARQUET') as query1

)

CREATE View Gold.Product_Categories  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Product_Categories/',FORMAT='PARQUET') as query1

)

CREATE View Gold.Products  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Products/',FORMAT='PARQUET') as query1

)

CREATE View Gold.Returns  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Returns/',FORMAT='PARQUET') as query1

)

CREATE View Gold.Sales  as (
select * from OPENROWSET(BULK 'https://awdatastoragelake.blob.core.windows.net/silver/AdventureWorks_Sales/',FORMAT='PARQUET') as query1

)

