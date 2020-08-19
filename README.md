# TakeAway-Data-Aggregation

The task is to read the data file into Spark and calculate the _net_ merchandise value of the order(ed products).

The Schema of input data looks like:

         root
          |-- customerId: string (nullable = true)
          |-- orders: array (nullable = true)
          |    |-- element: struct (containsNull = true)
          |    |    |-- basket: array (nullable = true)
          |    |    |    |-- element: struct (containsNull = true)
          |    |    |    |    |-- grossMerchandiseValueEur: double (nullable = true)
          |    |    |    |    |-- productId: string (nullable = true)
          |    |    |    |    |-- productType: string (nullable = true)
          |    |    |-- orderId: string (nullable = true)


When computing the net value, take the following things into consideration:
There is a 7% VAT applied to cold foods, 15% to hot foods and 9% on beverages.
