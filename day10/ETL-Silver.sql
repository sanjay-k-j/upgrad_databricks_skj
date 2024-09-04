-- Databricks notebook source
-- MAGIC %run /Workspace/Users/sanjay_1724384809981@npupgradassessment.onmicrosoft.com/day10/day8/includes

-- COMMAND ----------

create table if not exists silver.products_silver as (select ProductID as product_id, ProductName as product_name ,Category as category, ListPrice as list_price from bronze.products_bronze)

-- COMMAND ----------

create table if not exists gold.category_count as select category, count(*) as count from silver.products_silver group by category order by count desc

-- COMMAND ----------

select * from gold.category_count
