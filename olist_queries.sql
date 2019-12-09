-- The goal of Capstone #3 is to 
-- A): investigate the data using graphs, tableau, excel, python, sql, whatever you want, and 
-- B): to try to build a regression model to predict how much a new customer will spend in their first six months. 

-- Based on A), you are also free to go in other directions or build different models in addition to B) above, with the caveat that your preliminary presentation and slides are due on FRIDAY.  Make it work. 



CREATE TABLE customer(
	customer_id varchar(255) NOT NULL
	,customer_unique_id varchar(255) NOT NULL
	,customer_zip_code_prefix integer NOT NULL
	,customer_city varchar(255) NOT NULL
	,customer_state varchar(255) NOT NULL
	,PRIMARY KEY(customer_id)

COPY
	customer

FROM 
	'/Users/galvanize/Documents/regression/olist/olist_customers_dataset.csv'DELIMITER ',' CSV HEADER;





CREATE TABLE ordertest(
	order_id varchar(255) NOT NULL
	,customer_id varchar(255) NOT NULL
	,order_status varchar(255)
	,order_purchase_timestamp timestamp
	,order_approved_at timestamp
	,order_delivered_carrier_date timestamp
	,order_delivered_customer_date timestamp
	,order_estimated_delivery_date timestamp
	,PRIMARY KEY(order_id)
	,FOREIGN KEY (customer_id) REFERENCES customer(customer_id))


COPY
	ordertest

FROM 
	'/Users/galvanize/Documents/regression/olist/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;



CREATE TABLE order_item(
    order_id varchar(255) NOT NULL
    ,order_item_id varchar(255) NOT NULL
    ,product_id varchar(255) NOT NULL
    ,seller_id varchar(255) NOT NULL
    ,shipping_limit_date date NOT NULL
    ,price decimal
    ,freight_value decimal
    ,FOREIGN KEY (order_id) REFERENCES ordertest(order_id)
)

COPY order_item
FROM '/Users/galvanize/Documents/regression/olist/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;




WITH latest_purchase AS(

SELECT
	customer_id
	,MAX(order_purchase_timestamp) as last_purchase

FROM
	ordertest

GROUP BY 
	customer_id

ORDER BY
	2)


SELECT 
	COUNT(active) as type_customer
	,active

FROM
	(SELECT
	*
	,CASE WHEN last_purchase <= '2018-01-01'
	THEN 'inactive'
	ELSE 'active'
	END as active

FROM 
	latest_purchase) AS test

GROUP BY
	active


------

SELECT
	count(amount)
	,amount

FROM 
	payment

GROUP BY 
	amount

----

SELECT 
	customer_id 
	,COUNT(order_id)

FROM 
	ordertest

GROUP BY
	1

ORDER BY
	2 DESC
-----
ALTER TABLE ordertest
RENAME TO orders_dataset;

ALTER TABLE orders_dataset
RENAME TO order_dataset;
-----

CREATE TABLE order_payment(
	order_id varchar(255) NOT NULL
	,payment_sequential int
	,payment_type varchar(255) 
	,payment_installments int
	,payment_value decimal
	,FOREIGN KEY (order_id) REFERENCES order_dataset(order_id))

COPY
	order_payment

FROM 
	'/Users/galvanize/Documents/regression/olist/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

------

select count(*) from order_payment;
-- 103886
select count(*) from order_dataset;
-- 99441
select count(*) from customer limit 20;
-- 99441

select * from order_dataset limit 20;

select * from order_payment limit 20;

select * from customer limit 20;


WITH deliveries as(
select
od.order_id
,od.customer_id
,od.order_purchase_timestamp

from
order_dataset as od

WHERE
od.order_status = 'delivered')

select 
cust.customer_unique_id
,del.customer_id
,cust.customer_zip_code_prefix
,cust.customer_city
,cust.customer_state
,del.order_id
,del.order_purchase_timestamp

from
customer as cust
right join deliveries as del
on cust.customer_id = del.customer_id

-- ^count is 96478, same as the count of rows of just orders that have a status of delivered


WITH deliveries as(
select
od.order_id
,od.customer_id
,od.order_purchase_timestamp

from
order_dataset as od

WHERE
od.order_status = 'delivered')

,cust_deliveries as (
select 
cust.customer_unique_id
,del.customer_id
,cust.customer_zip_code_prefix
,cust.customer_city
,cust.customer_state
,del.order_id
,del.order_purchase_timestamp

from
customer as cust
right join deliveries as del
on cust.customer_id = del.customer_id)

,order_amounts as (select
count(customer_id) as total_orders
,customer_unique_id

from cust_deliveries

group by 2

order by 1 DESC)

select 
* 
from
order_amounts

where 
total_orders>1

-- ^2801 customers that have ordered more than once

CREATE TABLE current_customer as(
WITH deliveries as(
select
od.order_id
,od.customer_id
,EXTRACT(year from od.order_purchase_timestamp) as latest_year

from
order_dataset as od

WHERE
od.order_status = 'delivered'
AND
EXTRACT(year from od.order_purchase_timestamp) = 2018)

,cust_deliveries as (
select 
cust.customer_unique_id
,del.customer_id
,cust.customer_zip_code_prefix
,cust.customer_city
,cust.customer_state
,del.order_id
,del.latest_year

from
customer as cust
right join deliveries as del
on cust.customer_id = del.customer_id)

,order_amounts as (select
count(customer_id) as total_orders
,customer_unique_id

from cust_deliveries

group by 2

order by 1 DESC)

select 
* 
from
order_amounts as oa

where 
total_orders>1)

--^ 1086 or so customers that have bought in the last year of the dataset and their amount of orders

CREATE TABLE current_customer as(
WITH deliveries as(
select
od.order_id
,od.customer_id
,EXTRACT(year from od.order_purchase_timestamp) as latest_year

from
order_dataset as od)

-- IT DOESN'T MATTER IF THEY ORDERED MULTIPLE ITEMS IN THE SAME ORDER. CUT ORDERS

SELECT product_id, 
count (distinct seller_id) 

FROM order_item 
group by product_id 
order by 2 DESC

-- ^ Shows the amount of sellers that sell the same product id

ALTER TABLE order_item
ALTER COLUMN order_item_id TYPE int USING order_item_id::integer;

--^ change data type from a varchar to integer

WITH ten_customer as(
select * from customer
where customer_unique_id = '8d50f5eadf50201ccdcedfb9e2ac8455'
OR customer_unique_id = '3e43e6105506432c953e165fb2acf44c'
OR customer_unique_id = 'ca77025e7201e3b30c44b472ff346268'
OR customer_unique_id = '6469f99c1f9dfae7733b25662e7f1782'
OR customer_unique_id = '1b6c7548a2a1f9037c1fd3ddfed95f33'
OR customer_unique_id = 'dc813062e0fc23409cd255f7f53c7074'
OR customer_unique_id = 'de34b16117594161a6a89c50b289d35a'
OR customer_unique_id = 'f0e310a6839dce9de1638e0fe5ab282a'
OR customer_unique_id = '63cfc61cee11cbe306bff5857d00bfe4'
OR customer_unique_id = '47c1a3033b8b77b3ab6e109eb4d5fdf3')

,cust_order AS(
select 
customer_unique_id
,customer_city
,customer_state
,order_id
,od.customer_id
,order_status
,order_purchase_timestamp

from order_dataset as od
JOIN ten_customer as tc
ON od.customer_id = tc.customer_id)

,quantities as(
select 
customer_unique_id
,customer_city
,customer_state
,co.order_id
,order_status
,order_purchase_timestamp
,product_id
,price
,order_item_id

from order_item as oi
JOIN cust_order as co
ON oi.order_id = co.order_id

order by order_id)

,amount_item as(
select 
customer_unique_id
,customer_city
,customer_state
,q.order_id
,order_status
,order_purchase_timestamp
,product_id
,price
,payment_value
,MAX(order_item_id) as item_count

from quantities as q
JOIN order_payment as op
ON q.order_id = op.order_id

group by 1,2,3,4,5,6,7,8,9)

,purchase_date as(
select 
customer_unique_id
,customer_city
,customer_state
,order_id
,payment_value
,order_purchase_timestamp

from amount_item)

,six_month as (
SELECT 
*
,order_purchase_timestamp + INTERVAL '6 months' as six_months

from
purchase_date)

SELECT 
customer_unique_id
,customer_city
,customer_state
,order_id
,payment_value
,EXTRACT(year from order_purchase_timestamp) as original_order_year
,EXTRACT(month from order_purchase_timestamp) as original_order_month
,EXTRACT(year from six_months) as projected_year
,EXTRACT(month from six_months) as projected_month

from 
six_month

----^ an attempt to add columns with order_item_id and other (i thought) pertinent data while trying to add six months to the first purchase timestamp

WITH ten_customer as(
select * from customer
where customer_unique_id = '8d50f5eadf50201ccdcedfb9e2ac8455'
OR customer_unique_id = '3e43e6105506432c953e165fb2acf44c'
OR customer_unique_id = 'ca77025e7201e3b30c44b472ff346268'
OR customer_unique_id = '6469f99c1f9dfae7733b25662e7f1782'
OR customer_unique_id = '1b6c7548a2a1f9037c1fd3ddfed95f33'
OR customer_unique_id = 'dc813062e0fc23409cd255f7f53c7074'
OR customer_unique_id = 'de34b16117594161a6a89c50b289d35a'
OR customer_unique_id = 'f0e310a6839dce9de1638e0fe5ab282a'
OR customer_unique_id = '63cfc61cee11cbe306bff5857d00bfe4'
OR customer_unique_id = '47c1a3033b8b77b3ab6e109eb4d5fdf3'
)

,cust_order AS(
select 
customer_unique_id
,customer_city
,customer_state
,order_id
,od.customer_id
,order_status
,order_purchase_timestamp

from order_dataset as od
JOIN ten_customer as tc
ON od.customer_id = tc.customer_id)


,total_purchase as(
select 
customer_unique_id
,customer_city
,customer_state
,co.order_id
,order_status
,order_purchase_timestamp
,sum(payment_value) as total_order_price

from cust_order as co
JOIN order_payment as op
ON co.order_id = op.order_id

group by 1,2,3,4,5,6)

,first_purchase as(
select 
min(order_purchase_timestamp) as first_order
,customer_unique_id

from total_purchase

group by customer_unique_id)

,six_month_range as(
select customer_unique_id, first_order, first_order + INTERVAL '6 months' as six_months_later
from first_purchase)

select smr.customer_unique_id, 
first_order, 
six_months_later, 
order_purchase_timestamp
,order_id
,total_order_price

from 
six_month_range as smr
JOIN total_purchase as tp
ON smr.customer_unique_id = tp.customer_unique_id

where
order_purchase_timestamp between first_order and six_months_later

--^ the ten customers are ten people with the most orders. adding six months to everyone's first purchase date, and showing the orders that were made between that six month period, with the payment value made

CREATE TABLE six_month_purchases as (
WITH ten_customer as(
select * from customer
-- where customer_unique_id = '8d50f5eadf50201ccdcedfb9e2ac8455'
-- OR customer_unique_id = '3e43e6105506432c953e165fb2acf44c'
-- OR customer_unique_id = 'ca77025e7201e3b30c44b472ff346268'
-- OR customer_unique_id = '6469f99c1f9dfae7733b25662e7f1782'
-- OR customer_unique_id = '1b6c7548a2a1f9037c1fd3ddfed95f33'
-- OR customer_unique_id = 'dc813062e0fc23409cd255f7f53c7074'
-- OR customer_unique_id = 'de34b16117594161a6a89c50b289d35a'
-- OR customer_unique_id = 'f0e310a6839dce9de1638e0fe5ab282a'
-- OR customer_unique_id = '63cfc61cee11cbe306bff5857d00bfe4'
-- OR customer_unique_id = '47c1a3033b8b77b3ab6e109eb4d5fdf3'
)

,cust_order AS(
select 
customer_unique_id
,customer_city
,customer_state
,order_id
,od.customer_id
,order_status
,order_purchase_timestamp

from order_dataset as od
JOIN ten_customer as tc
ON od.customer_id = tc.customer_id)


,total_purchase as(
select 
customer_unique_id
,customer_city
,customer_state
,co.order_id
,order_status
,order_purchase_timestamp
,sum(payment_value) as total_order_price

from cust_order as co
JOIN order_payment as op
ON co.order_id = op.order_id

group by 1,2,3,4,5,6)

,first_purchase as(
select 
min(order_purchase_timestamp) as first_order
,customer_unique_id

from total_purchase

group by customer_unique_id)

,six_month_range as(
select customer_unique_id, first_order, first_order + INTERVAL '6 months' as six_months_later
from first_purchase)

select smr.customer_unique_id
,customer_city
,customer_state
,first_order 
,six_months_later
,order_purchase_timestamp
,order_id
,total_order_price

from 
six_month_range as smr
JOIN total_purchase as tp
ON smr.customer_unique_id = tp.customer_unique_id

where
order_purchase_timestamp between first_order and six_months_later)
copy six_month_purchases
to '/Users/galvanize/Documents/regression/olist/six_month_purchase.csv' DELIMITER ',' CSV HEADER
--^ just what i had in pgadmin before i closed it