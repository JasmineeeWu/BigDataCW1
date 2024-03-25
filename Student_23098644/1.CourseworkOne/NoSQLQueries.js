/*
Author: Student_23098644
Date: 2024-03-25
Content: NoSQL - MongoDB Queries for CourseWork One
*/
/* NoSQL Query 1: Calculate the average price for each GICSSector when the security market cap is larger than 100000, find the best-performed sector.*/

db.CourseworkOne.aggregate([
    {$match:{"MarketData.MarketCap":{$gt:100000}}},
    {$group:{_id:'$StaticData.GICSSector', average:{$avg:'$MarketData.Price'}}},
    {$sort:{average: -1}},
    {$limit:1}])


/* NoSQL Query 2: A Personalised Security Screening Process: find the best security in each GICSSector that satisfy: 
1. Market Cap above sector average;
2. PE Ratio above sector average;
3. Dividend Yield above sector average;
4. Choose the lowest-Beta security after step 1-3.
Approach:
Step 1: Filter out the securities data with 'NA' or 'null'
Step 2: Calculate sector average Market Cap, PE Ratio, and Dividend Yield
Step 3: Filter chosen security based on criteria
Step 4: Sort by Beta within each GICSSector
Step 5: Choose the ones with the smallest Beta in each sector */

db.CourseworkOne.aggregate([
    {$match:{ 
        "MarketData.MarketCap":{$nin:["NA", null]}, 
        "MarketData.Beta":{$nin:["NA", null]}, 
        "FinancialRatios.DividendYield":{$nin:["NA", null]}, 
        "FinancialRatios.PERatio":{$nin:["NA", null]}}},

    {$group:{ 
        _id:"$StaticData.GICSSector", 
        avgPE:{$avg:"$FinancialRatios.PERatio"}, 
        avgDY:{$avg:"$FinancialRatios.DividendYield"}, 
        avgMCap:{$avg:"$MarketData.MarketCap"}, 
        securities:{$push:{symbol:"$Symbol", MarketCap:"$MarketData.MarketCap", PE:"$FinancialRatios.PERatio", DividendYield:"$FinancialRatios.DividendYield", Beta:"$MarketData.Beta"}}}},
    
    {$project:{
         _id:1, chosensecurity:{$arrayElemAt:[{$filter:{input:"$securities", as:"security", 
         cond:{$and:[ 
            {$gt:["$$security.MarketCap", "$avgMCap"]}, 
            {$gt:["$$security.PE", "$avgPE"]}, 
            {$gt:["$$security.DividendYield", "$avgDY"]}]}}}, 0]}}},
    
    {$sort:{"_id":1, "chosensecurity.Beta":1}},
    {$group:{_id:"$_id", chosensecurity:{$first:"$chosensecurity"}}},
    {$project:{_id:1, "chosensecurity.symbol":1, "chosensecurity.Beta":{$min:"$chosensecurity.Beta"}}}])
