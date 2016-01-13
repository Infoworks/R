# R
This repo contains R files.

# R scripts
## spark_api.R

It is spark SQL API. 
### Functions
####Select
Select table and subset of columns. Also create new columns.
##### Parameters

*   Table name - Mandatory
*   Boolean optional flag (TRUE or FALSE) to indicate if original columns should be kept or not - Optional
*	Column names - Mandatory

####Join
Usage
jdf1 = Join(tpcds1gb.store_sales, tpcds1gb.store, tpcds1gb.customer)

# Data Files
## NWDW.json

It is json data. It contain orders. Orders has data about order, customer, shipping information. It has sales employee information and the territories that the employee covers. Each territory details is provided. 

It also has Customers details. Id and complete address of the customers is provided.

OrderDetails has information about OrderDetails like Product, Categories, Shipping details etc. 

