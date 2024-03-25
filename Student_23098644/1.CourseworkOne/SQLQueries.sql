/*
Author: Student_23098644
Date: 2024-03-25
Content: SQL Queries for CourseWork One
*/
set search_path = cash_equity, "$user", public;
-- SQL Query 1: Compare the US and France equity performance from different GICS sectors.
with sector_return as (
select gics_sector, country,
       round(avg(((close_price - open_price) / open_price) * 100),4) as daily_return
from equity_static 
left join equity_prices on equity_static.symbol = equity_prices.symbol_id
where country in ('US','FR')
group by equity_static.gics_sector, equity_static.country)

select gics_sector,
round(avg(case when country = 'US' then daily_return end),4) as US_return,
round(avg(case when country = 'FR' then daily_return end),4) as FR_return,

case
when avg(case when country = 'US' then daily_return end) > avg(case when country = 'FR' then daily_return end) then 'US'
when avg(case when country = 'US' then daily_return end) < avg(case when country = 'FR' then daily_return end) then 'FR'
else 'N/A'
end as Comparison

from sector_return
group by gics_sector
having count(distinct country) > 1
order by gics_sector;



-- SQL Query 2: Compare the performance for each fund from 2023-10-28 to 2023-11-24 (assume portfolio positions did not change).
with trader_performance as(
    with portfolio_equity as (
        select equity_prices.cob_date, equity_prices.currency, 
               portfolio_positions.trader, portfolio_positions.symbol, portfolio_positions.net_amount, portfolio_positions.net_quantity
        from equity_prices
        left join portfolio_positions on equity_prices.symbol_id = portfolio_positions.symbol
        where equity_prices.cob_date between '2023-10-28' and '2023-11-24' 
        group by equity_prices.cob_date, equity_prices.currency, trader, symbol, net_amount, net_quantity
        order by equity_prices.cob_date)
    select portfolio_equity.cob_date, portfolio_equity.currency, trader, fund_name, fund_focus, trader_name, symbol, net_amount, net_quantity, close_price, 
           close_price * net_quantity as new_net_amount, 
           round((((close_price * net_quantity) - net_amount)/net_amount)*100, 4)as amount_change
    from portfolio_equity
    right join equity_prices on portfolio_equity.cob_date = equity_prices.cob_date and portfolio_equity.symbol = equity_prices.symbol_id
    left join trader_static on portfolio_equity.trader = trader_static.trader_id
    where portfolio_equity.trader <> 'null'
    group by portfolio_equity.cob_date, portfolio_equity.currency, trader, fund_name, fund_focus, trader_name, symbol, net_amount, net_quantity, close_price
    order by trader_name)

select trader_name, fund_name, fund_focus, avg(amount_change) as average_change
from trader_performance
group by trader_name, fund_name, fund_focus
order by average_change DESC;
