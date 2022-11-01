SELECT * FROM commodity_prices.ice_utility_markets
WHERE hub_name = 'TTF'
    AND trade_date = '05/18/2022'
    AND contract_date = 'monthly future'
    AND relative_contract_type = 'monthly future' 
    AND contract_order <= 12