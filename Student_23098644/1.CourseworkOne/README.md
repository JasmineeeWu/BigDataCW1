# My first coursework

- [My first coursework](#my-first-coursework)
  - [Introduction](#introduction)
  - [Description of SQL and NoSQL Databases](#description-of-sql-and-nosql-databases)
    -[SQL Database](#sql-database)
    -[NoSQL Database](#nosql-database)
  - [SQL Query Explain](#sql-query-explain)
    -[SQL1](#sql1)
    -[SQL2](#sql2)
  - [NoSQL Query Explain](#nosql-query-explain)
    -[NoSQL1](#nosql1)
    -[NoSQL2](#nosql2)
  - [Conclusion](#conclusion)

## Introduction

Query tools can be used to analyse databases and provide users with powerful capabilities to derive insights from data. This report will demonstrate practical applications for given financial databases using SQL and NoSQL techniques. They have different ways, but have equally powerful and effective capabilities for solving data problems. By exploring both query tools with real-world data, this report addresses a total of four real-world financial problems, including: 
1.	Compare equity performance across countries and sectors in SQL, 
2.	Compare performance of funds and fund traders in SQL, 
3.	Compare the performance of large cap securities across sectors in NoSQL, 
4.	A personalised method for screening securities in NoSQL.


## Description of SQL and NoSQL Databases
## SQL Database

SQL databases organise data into tables with columns. The given SQL database contains six tables in the schema name called ‘cash_equity’. 

Table ‘equity_prices’ presents equity basic financial data from the date 2021-01-04 to 2023-11-23 including the equity's daily prices, volume, currency, and IDs. 
Table ‘equity_static’ shows the equity basic fundamental information including security names, GICS sectors, GICS industries, countries, and regions. 
Table ‘exchange_rates’ summaries the daily exchange rate across different currencies. 
Table ‘portfolio_postiions’ provides statistics for the portfolio position changes on 2023-10-27, with trader information, net quantity, and net amount for each position change. 
Table ‘trader_limits’ lists the traders, and their trade limit types, categories, and amounts with limit date periods. 
The last table ‘trader_static’ contains 10 columns that present different trader names and their different managed funds names, types, and focuses, etc.
The SQL database contains both special and common information, which in general gives the investor a lot of information to analyse.

## NoSQL Database

NoSQL databases can record data in various formats like in a JSON file, which are more flexible. The given NoSQL database records 505 securities and their basic information respectively. Each security’s information contains symbols, static data, market data, and financial ratios. StaticData includes security name, SEC filings, GICS sector, and GICS sub-industry. MarketData includes price, market cap, and beta. FinancialRatios includes dividend yield, PE ratio, and payout ratio. 

The NoSQL database provides the security information in sections, which means it is more intuitive to see the basic information about each security, as the information structure is essentially the same in each section. Note that the NoSQL database is going to apply in MongoDB, in a collection called ‘CoureworkOne’. 

## SQL Query Explain
## SQL1
#### Background: 
Understanding how equity returns vary across different sectors from different countries allows investors to not only get a comprehensive overview of the global equity market trend but also helps investors better diversify their portfolios and seek more investment opportunities.
#### Aims: 
The primary aim of the query is to compare the average returns of US and France equities across various GICS sectors. The query demonstrates the average returns, as well as illustrates a comparison between the two countries’ equity performance at the same time. 
#### Approach: 

```
set search_path = cash_equity, "$user", public;
```

The general approach is summarised as follows:

•	Step 1: Started by formulating a new derived table called sector_return. The sector return table includes the average return (daily_return) calculated by (closing price-open price)/open price for each sector in the two countries respectively. 

```
with sector_return as (
select gics_sector, country,
       round(avg(((close_price - open_price) / open_price) * 100),4) as daily_return
from equity_static 
left join equity_prices on equity_static.symbol = equity_prices.symbol_id
where country in ('US','FR')
group by equity_static.gics_sector, equity_static.country)
```

•	Step 2: Aggregate the average return based on the country and add a column to compare the us_return and fr_return in different sectors. 

```
select gics_sector,
round(avg(case when country = 'US' then daily_return end),4) as US_return,
round(avg(case when country = 'FR' then daily_return end),4) as FR_return,
case
when avg(case when country = 'US' then daily_return end) > avg(case when country = 'FR' then daily_return end) then 'US'
when avg(case when country = 'US' then daily_return end) < avg(case when country = 'FR' then daily_return end) then 'FR'
else 'N/A'
end as Comparison
```

•	Step 3: Filter out those N/A by having count (distinct country) larger than one, since the output shows that there are sectors that cannot be compared because of the lack of sector data.

```
from sector_return
group by gics_sector
having count(distinct country) > 1
order by gics_sector;
```
#### Output: 
The output table has four columns gics_sector, us_return, fr_return, and comparison. The gics_sector column shows ten sectors including Communication Services, Consumer Discretionary, Consumer Staples, Energy, Financials, Health Care, Industries, Materials, Real Estate, and Utilities. Note that there are two sectors (Information Technology and Technology) that are not included in the output as the missing return (‘null’) of one of the countries. 
It can be discovered from the output table that for most sectors, France outperforms the US, however, for the Energy and Financials sector, the US exceeds France with a higher average return. The comparison provides valuable insight into the relative performance of US and France equities and helps inform investment decisions or portfolio adjustments.

## SQL2

#### Background: 
Each fund trader manages a fund. It is important to understand how the fund traders perform by directly discovering their fund performance, since it will allow investors to assess the traders’ effectiveness as well as guide their future investment decisions. 
#### Aims: 
The primary aim of the query is to assess and compare the performance of individual funds during the period from 2023-10-28 to 2023-11-24. By analyzing changes in the net amount of equity positions held by each fund over this timeframe, the query will enable investors to gain insights into the relative performance of the given funds and at the same time, evaluate the fund trader's performance.
#### Approach: 
When discovering the raw database, I found that the portfolio position changes occurred on 2023-10-27, so I would like to research the traders’ performance and their corresponding fund performance assuming that the portfolio positions did not change from 2023-10-28 to the end of the given period 2023-11-24. 


•	Step 1: Generate a derived table named trader_performance, including a derived table called portfolio_equity that extracts all position-changed equities’ currency, symbol, their traders, and the changing amount with quantity. 

```
with portfolio_equity as (
        select equity_prices.cob_date, equity_prices.currency, 
               portfolio_positions.trader, portfolio_positions.symbol, portfolio_positions.net_amount, portfolio_positions.net_quantity
        from equity_prices
        left join portfolio_positions on equity_prices.symbol_id = portfolio_positions.symbol
        where equity_prices.cob_date between '2023-10-28' and '2023-11-24' 
        group by equity_prices.cob_date, equity_prices.currency, trader, symbol, net_amount, net_quantity
        order by equity_prices.cob_date)
```

•	Step 2: Complete the trader_perfomance table construction by calculating these equities’ net amount change in the 1 month and including their trader names and fund names. Note that the new_net_amount is calculated using the daily close prices multiplied by the net quantity (assume no change). 

```
with trader_performance as(
--- portfolio_equity table insert here ---
    select portfolio_equity.cob_date, portfolio_equity.currency, trader, fund_name, fund_focus, trader_name, symbol, net_amount, net_quantity, close_price, 
           close_price * net_quantity as new_net_amount, 
           round((((close_price * net_quantity) - net_amount)/net_amount)*100, 4)as amount_change
    from portfolio_equity
    right join equity_prices on portfolio_equity.cob_date = equity_prices.cob_date and portfolio_equity.symbol = equity_prices.symbol_id
    left join trader_static on portfolio_equity.trader = trader_static.trader_id
    where portfolio_equity.trader <> 'null'
    group by portfolio_equity.cob_date, portfolio_equity.currency, trader, fund_name, fund_focus, trader_name, symbol, net_amount, net_quantity, close_price
    order by trader_name)
```

•	Step 3: Group the trader names, fund names, fund focuses, and calculate the average amount of change during the period to indicate the performance. 

```
select trader_name, fund_name, fund_focus, avg(amount_change) as average_change
from trader_performance
group by trader_name, fund_name, fund_focus
```

•	Step 4: Sort the result by average amount of change from the highest to lowest.

```
order by average_change DESC;
```
#### Output: 
The output table has three columns named trader_name, fund_name, and average_change. One trader manages one fund and there are a total of five funds that have portfolio position changes on 2023-10-27. To discover the output, fund Global Tech managed by Dan Green had the best performance across the five funds, and fund European High Momentum managed by Matt Red had the lowest average portfolio net amount change. From the output, it can also be concluded that during this 1 month, the information technology sector may have a good trend to invest, and the European stocks are not possibly recommended to invest. 


## NoSQL Query explain
## NoSQL1
#### Background: 
Typically, investors with an objective of income generation tend to invest in comparable large-cap companies with more stable returns than mid and small-cap companies. Those investors need to understand how relatively large companies perform in each GICS sector, to help them make decisions and seek more investment opportunities.
#### Aims: 
The query aims to calculate the average price for each GICSSector for securities with a relatively large market capitalization (assume larger than 100000 here) and find the best-performing sector among all sectors.
#### Approach:
```
Mongosh
use database
```

•	Step 1: Filter the documents where the market cap is larger than 100000.

```
db.CourseworkOne.aggregate([
    {$match:{"MarketData.MarketCap":{$gt:100000}}},
```

•	Step 2: Group the documents by GICS sectors and calculate the average price in each group.

```
{$group:{_id:'$StaticData.GICSSector', average:{$avg:'$MarketData.Price'}}},
```

•	Step 3: Sort the groups by the calculated average price in descending order and limit the output to the first one to compare and identify the best-performed sector with the highest large-cap average price. 

```
{$sort:{average: -1}},
{$limit:1}])
```
#### Output: 
The output result is [{id: 'Consumer Discretionary', average: 572.0925}], which means that the sector Consumer Discretionary has the highest average price when the market cap is relatively large (larger than 100000). If I only run the code until the $group, it will have 11 sectors with different large-cap average prices as well. It guided the income-generating investors that the Consumer Discretionary sector is the most worth investing in.

## NoSQL2
#### Background: 
It is essential to generate a personalised security screening process when formulating portfolios. The criteria the investors may consider include market capitalisation, PE ratio, dividend yield, beta, etc. Designing a query template for the security chosen helps not only investors but also analysts construct a portfolio with the consideration of specific fundamental and risk criteria.
#### Aims: 
The query aims to identify the best security in each GICS sector that meets the following criteria: 
1.	Market Cap above the sector average.
2.	PE Ratio above the sector average.
3.	Dividend yield above the sector average.
4.	Find a security with the lowest beta among those satisfying criteria 1-3.
#### Approach: 
The approach can be summarised as follows:

•	Step 1: Filter out the securities data with ‘NA’ or ‘null’ to make sure a valid output using $match.

```
db.CourseworkOne.aggregate([
    {$match:{ 
        "MarketData.MarketCap":{$nin:["NA", null]}, 
        "MarketData.Beta":{$nin:["NA", null]}, 
        "FinancialRatios.DividendYield":{$nin:["NA", null]}, 
        "FinancialRatios.PERatio":{$nin:["NA", null]}}},
```

•	Step 2: Calculate and group each sector average Market Cap, PE Ratio, and Dividend Yield for further used criteria.

```
 {$group:{ 
        _id:"$StaticData.GICSSector", 
        avgPE:{$avg:"$FinancialRatios.PERatio"}, 
        avgDY:{$avg:"$FinancialRatios.DividendYield"}, 
        avgMCap:{$avg:"$MarketData.MarketCap"}, 
        securities:{$push:{symbol:"$Symbol", MarketCap:"$MarketData.MarketCap", PE:"$FinancialRatios.PERatio", DividendYield:"$FinancialRatios.DividendYield", Beta:"$MarketData.Beta"}}}},
```

•	Step 3: Filter chosen security based on the criteria using $project.

```
{$project:{_id:1, chosensecurity:{$arrayElemAt:[{$filter:{input:"$securities", as:"security", cond:{$and:[ 
            {$gt:["$$security.MarketCap", "$avgMCap"]}, 
            {$gt:["$$security.PE", "$avgPE"]}, 
            {$gt:["$$security.DividendYield", "$avgDY"]}]}}}, 0]}}},
```

•	Step 4: Sort the previous result by Beta for each sector using $sort.

```
{$sort:{"_id":1, "chosensecurity.Beta":1}},{$group:{_id:"$_id", chosensecurity:{$first:"$chosensecurity"}}},
```

•	Step 5: Choose the ones with the smallest Beta in each sector using $min.

```
{$project:{_id:1, "chosensecurity.symbol":1, "chosensecurity.Beta":{$min:"$chosensecurity.Beta"}}}])
```
#### Output: 
The output obtained from the query illustrates that for the given 11 GICS sectors, there are two sectors (Consumer Discretionary, Real Estate, and Communication Services) that cannot screen a best security meeting the criteria. For other sectors, there are 8 securities being chosen, with different betas as well. Typically, Industrials, Financials, and Information Technology sectors have relatively higher risks than the market while the beta of other sector securities are lower than one. Therefore, if the investor includes those securities in the portfolio, the portfolio will show significant diversification due to different sectors and different risks.


## Conclusion

In conclusion, the query-design process solved four real-life financial and investment information that may be encountered and explored. 

From the SQL queries, it can be summarised that for the Energy and Financials sector, the US equities exceed France's equities and for other GICS sectors, France's equities outperform the US equities. Investors can consider diversifying their portfolios from the country's perspective. Furthermore, considering the performance of the given funds, the fund Global Tech (information technology sector) may have a good trend to invest, and the fund European High Momentum (European stocks) is not possibly recommended to invest. 

From the NoSQL queries, it can be concluded that the Consumer Discretionary sector is the most worth investing in for income-generating investors. Also, from the personal perspective of screening the securities, investors are recommended to include ‘CVX’, ‘BA’, ‘APD’, ‘KO’, ‘BLK’, ‘ABBV’, ‘AVGO’, and ‘D’ in their portfolio, which will provide great diversification effect and income-generating effects.

