CREATE TABLE london_daily_glass_stock AS

WITH ldn_drink_sales AS (
SELECT 
    date_created,
    bar_no,
    london_daily_transactions.drink,
    glass_type,
    total_amount,
    drinks_sold
FROM london_daily_transactions
LEFT JOIN glass_types_all_cocktails ON 
    glass_types_all_cocktails.drink = london_daily_transactions.drink
),

stock AS(
SELECT
    glass_stocks_all_bars.bar,
    glass_type,
    stock,
    bar_locations.bar_no
FROM glass_stocks_all_bars
LEFT JOIN bar_locations ON glass_stocks_all_bars.bar = bar_locations.bar
)

SELECT
    date_created,
    bar_no,
    glass_type,
    total_amount,
    drinks_sold,
    stock,
    stock - drinks_sold AS remaining_glass_stock
FROM(
    SELECT 
        date_created,
        ldn_drink_sales.bar_no,
        ldn_drink_sales.drink,
        ldn_drink_sales.glass_type,
        total_amount,
        drinks_sold,
        stock
    FROM ldn_drink_sales
    LEFT JOIN stock ON 
        ldn_drink_sales.bar_no = stock.bar_no AND ldn_drink_sales.glass_type = stock.glass_type
)
ORDER BY 
    date_created;


CREATE TABLE budapest_daily_glass_stock AS

WITH bud_drink_sales AS (
SELECT 
    date_created,
    bar_no,
    budapest_daily_transactions.drink,
    glass_type,
    total_amount,
    drinks_sold
FROM budapest_daily_transactions
LEFT JOIN glass_types_all_cocktails ON 
    budapest_daily_transactions.drink = glass_types_all_cocktails.drink
),

stock AS(
SELECT
    glass_stocks_all_bars.bar,
    glass_type,
    stock,
    bar_locations.bar_no
FROM glass_stocks_all_bars
LEFT JOIN bar_locations ON glass_stocks_all_bars.bar = bar_locations.bar
)

SELECT
    date_created,
    bar_no,
    glass_type,
    total_amount,
    drinks_sold,
    stock,
    stock - drinks_sold AS remaining_glass_stock
FROM(
    SELECT 
        date_created,
        bud_drink_sales.bar_no,
        bud_drink_sales.drink,
        bud_drink_sales.glass_type,
        total_amount,
        drinks_sold,
        stock
    FROM bud_drink_sales
    LEFT JOIN stock ON 
        bud_drink_sales.bar_no = stock.bar_no AND bud_drink_sales.glass_type = stock.glass_type
)
ORDER BY 
    date_created;


CREATE TABLE new_york_daily_glass_stock AS

WITH ny_drink_sales AS (
SELECT 
    date_created,
    bar_no,
    new_york_daily_transactions.drink,
    glass_type,
    total_amount,
    drinks_sold
FROM new_york_daily_transactions
LEFT JOIN glass_types_all_cocktails ON 
    new_york_daily_transactions.drink = glass_types_all_cocktails.drink
),

stock AS(
SELECT
    glass_stocks_all_bars.bar,
    glass_type,
    stock,
    bar_locations.bar_no
FROM glass_stocks_all_bars
LEFT JOIN bar_locations ON glass_stocks_all_bars.bar = bar_locations.bar
)

SELECT
    date_created,
    bar_no,
    drink,
    glass_type,
    total_amount,
    drinks_sold,
    stock,
    stock - drinks_sold AS remaining_glass_stock
FROM(
    SELECT 
        date_created,
        ny_drink_sales.bar_no,
        ny_drink_sales.drink,
        ny_drink_sales.glass_type,
        total_amount,
        drinks_sold,
        stock
    FROM ny_drink_sales
    LEFT JOIN stock ON 
        ny_drink_sales.bar_no = stock.bar_no AND ny_drink_sales.glass_type = stock.glass_type
)
ORDER BY 
    date_created;