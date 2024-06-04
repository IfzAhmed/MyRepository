# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table ada_ecommerce.ada_ecomm_gold.churnx as
# MAGIC select country,channel,store,--category,sub_category,product_name,
# MAGIC concat(date_format(last_churn_update_date,'MMM'),' ',date_format(last_churn_update_date,'yy')) as cohort,
# MAGIC count(distinct customer_id) as customer_count from 
# MAGIC ada_ecommerce.ada_ecomm_gold.Buyer_data_yy where
# MAGIC churn_flag = 1 
# MAGIC group by 1,2,3,4

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ada_ecommerce.ada_ecomm_gold.churny as
# MAGIC with cte as (
# MAGIC   select distinct last_day(order_date) as last 
# MAGIC   from ada_ecommerce.ada_ecomm_gold.Buyer_data_yy 
# MAGIC   order by 1
# MAGIC )
# MAGIC select country, channel, store, --category, sub_category, product_name, 
# MAGIC        concat(date_format(cte.last, 'MMM'), ' ', date_format(cte.last, 'yy')) as cohort, 
# MAGIC        Count(distinct cte1.customer_id) as active_customers 
# MAGIC from cte 
# MAGIC cross join (
# MAGIC   select distinct country, channel, store, --category, sub_category, product_name,
# MAGIC    order_date, customer_id 
# MAGIC   from ada_ecommerce.ada_ecomm_gold.Buyer_data_yy  --ada_ecommerce.ada_ecomm_gold.buyer_data_y1
# MAGIC ) cte1  
# MAGIC where cte1.order_date > cte.last - interval '13 months'
# MAGIC   and cte1.order_date < cte.last - interval '1 month' + interval '1 day'
# MAGIC group by 1, 2, 3, 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ada_ecommerce.ada_ecomm_gold.churnx

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ada_ecommerce.ada_ecomm_gold.buyer_data_finalx as
# MAGIC WITH buyerx AS (
# MAGIC     SELECT *,
# MAGIC            CONCAT(date_format(x.order_date, 'MMM'), ' ', date_format(x.order_date, 'yy')) AS cohort
# MAGIC     FROM ada_ecommerce.ada_ecomm_gold.Buyer_data_yy x
# MAGIC )
# MAGIC SELECT x.*,
# MAGIC coalesce(y.customer_count,0) as churned_customer_count,z.active_customers
# MAGIC FROM buyerx x
# MAGIC   LEFT JOIN ada_ecommerce.ada_ecomm_gold.churnx y
# MAGIC   ON x.country = y.country
# MAGIC   AND x.channel = y.channel
# MAGIC   AND x.store = y.store
# MAGIC   -- AND x.category = y.category
# MAGIC   -- AND x.sub_category = y.sub_category
# MAGIC   -- AND x.product_name = y.product_name
# MAGIC   AND x.cohort = y.cohort
# MAGIC LEFT JOIN ada_ecommerce.ada_ecomm_gold.churny z
# MAGIC   ON x.country = z.country
# MAGIC   AND x.channel = z.channel
# MAGIC   AND x.store = z.store
# MAGIC   -- AND x.category = z.category
# MAGIC   -- AND x.sub_category = z.sub_category
# MAGIC   -- AND x.product_name = z.product_name
# MAGIC   AND x.cohort = z.cohort;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ada_ecommerce.ada_ecomm_gold.buyer_final --ada_ecommerce.ada_ecomm_gold.buyer_data_finalx 
# MAGIC where customer_id = 15863844
# MAGIC order by order_date;

# COMMAND ----------


