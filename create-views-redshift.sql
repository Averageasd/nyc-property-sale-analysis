DROP MATERIALIZED VIEW IF EXISTS sales_by_borough;
CREATE MATERIALIZED VIEW sales_by_borough
AS
SELECT borough, SUM(sale_price) as sale_price_total
FROM public.nyc_properties_sale
GROUP BY borough;

DROP MATERIALIZED VIEW IF EXISTS sales_by_year;
CREATE MATERIALIZED VIEW sales_by_year
AS
SELECT EXTRACT(year FROM sale_date) as year_sale, SUM(sale_price) sale_price_total
FROM public.nyc_properties_sale
GROUP BY EXTRACT(year FROM sale_date);

REFRESH MATERIALIZED VIEW sales_by_borough;
REFRESH MATERIALIZED VIEW sales_by_year;