SELECT zip_code FROM 
processed WHERE zip_code = 0;

SELECT distinct borough FROM
processed;

SELECT 
borough, 
SUM(sale_price) total_sales_price_by_borough
FROM nyc_properties_sale_db.processed
group BY borough;

SELECT 
borough, 
count(sale_price) count_zero_sale
FROM nyc_properties_sale_db.processed
WHERE sale_price = 0
GROUP BY borough;

SELECT 
EXTRACT(year FROM sale_date) year_of_sale,
SUM(sale_price) sale_price_total
FROM processed
GROUP BY EXTRACT(year FROM sale_date);