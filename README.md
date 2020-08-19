# TakeAway-Data-Aggregation

The task is to read the data file into Spark and calculate the _net_ merchandise value of the order(ed products).

The Schema of input data looks like:
customerId:string
orders:array
element:struct
basket:array
element:struct
grossMerchandiseValueEur:double
productId:string
productType:string
orderId:string

When computing the net value, take the following things into consideration:
There is a 7% VAT applied to cold foods, 15% to hot foods and 9% on beverages.
