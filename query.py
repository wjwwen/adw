# Query into a spark dataframe 
% python
df = spark.sql("SELECT * FROM commodity_prices.ice_utility_markets \
               WHERE hub_name = 'TTF' AND trade_date = '05/18/2022' AND contract_type = 'monthly future'
               AND relative_contract_type = 'Monthly' AND contract_order <= 12")
display(df)