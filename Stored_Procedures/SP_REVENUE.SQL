USE [Feg_Reporting]
GO
/****** Object:  StoredProcedure [dbo].[prc_load_location_revenue]    Script Date: 6/22/2022 8:09:26 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[prc_load_location_revenue]

AS
BEGIN

 --### SET MONDAY AS DAY 1 OF THE WEEK  ###
SET DATEFIRST 1;
 
  --### CREATING DATA VARIABLES ###

 DECLARE
 
 @CY_yesterday     as date = dateadd(day,-1, cast(getdate() as date)),
 @CY_EreYesterday  as date = dateadd(day,-2, cast(getdate() as date)),
 @CY_StartOfYear   as date,
 @CY_PriorSaturday as date,
 @CY_StartOfMonth  as date,
 @CY_StartOfWeek   as date;
 
 DECLARE

 @PY_yesterday    as date,
 @PY_StartOfYear  as date,
 @PY_StartOfMonth as date,
 @PY_StartOfWeek  as date,
 @PY_DayOfWeek    as date;


 SELECT @CY_StartOfYear  = DATEADD(YEAR, DATEDIFF(YEAR, 0, @CY_yesterday), 0); 
 SELECT @CY_StartOfMonth = DATEADD(MONTH, DATEDIFF(MONTH, 0, @CY_yesterday), 0); 
-- SELECT @CY_StartOfWeek  = DATEADD(WEEK, DATEDIFF(WEEK, 0, @CY_yesterday), 0); 
 SELECT @CY_StartOfWeek  = CASE 
							WHEN DATEPART(WEEKDAY,GETDATE())=1 
							THEN    DATEADD(WEEK, DATEDIFF(WEEK, 0, @CY_yesterday), -7) 
							ELSE DATEADD(WEEK, DATEDIFF(WEEK, 0, @CY_yesterday), 0) 
							END;

  SELECT @CY_PriorSaturday =
    CASE 
	    WHEN DATEPART(WEEKDAY,@CY_yesterday) = 7
        THEN dateadd(day,-9, cast(@CY_StartOfWeek as date))
	    ELSE dateadd(day,-2, cast(@CY_StartOfWeek as date))
	END;



 SELECT @PY_yesterday = D.PY_DATE 
   FROM [Feg_Etl].[dbo].[Date_Dim] D
  WHERE D.FULL_DATE = @CY_yesterday;

 SELECT @PY_StartOfYear  = DATEADD(YEAR, DATEDIFF(YEAR, 0, @PY_yesterday), 0); 
 SELECT @PY_StartOfMonth = DATEADD(MONTH, DATEDIFF(MONTH, 0, @PY_yesterday), 0); 
 

 SELECT @PY_DayOfWeek = D.PY_DAY_OF_WEEK
   FROM [Feg_Etl].[dbo].[Date_Dim] D
  WHERE D.FULL_DATE = @CY_yesterday ;

--  SELECT @PY_StartOfWeek  = DATEADD(WEEK, DATEDIFF(WEEK, 0, @PY_DayOfWeek), 0);  
   SELECT @PY_StartOfWeek  = CASE 
							WHEN DATEPART(WEEKDAY,@PY_DayOfWeek  )=7 
							THEN    DATEADD(WEEK, DATEDIFF(WEEK, 0, @PY_DayOfWeek), -7) 
							ELSE DATEADD(WEEK, DATEDIFF(WEEK, 0, @PY_DayOfWeek), 0) 
							END;
	

  DECLARE @listOfLocations table (storeId int);
  INSERT @listOfLocations(storeId) values (2007),(2012),(2017),(2023),(2030),(2031),(2035),(2036),(2039),(2040),(2041),(2042),
                                          (2043),(2044),(2045),(2046),(2047),(2048),(2051),(2053),(2054),(2055),(2056),(2057),
										  (2058),(2060),(2061),(2062),(2063),(2064),(2065),(6002),(6003),(6004),(6005),(6006),
										  (6007),(6008),(6009),(6010),(6011),(6012),(6015),(6020),(6021),(6022),(6023),(6024),
										  (6025),(6026),(6027),(7001);   
										  





  --#### CREATING MATRIX WITH LOCATIONS AND GROUPSNAMES ###
  IF OBJECT_ID('tempdb..#listOfGroupNames') IS NOT NULL DROP TABLE #listOfGroupNames 

	select distinct ga.GroupName
	into #listOfGroupNames 
	FROM [Feg_Etl].[dbo].[sacoa_games_activity_master]  GA
	

  DECLARE @listOfFoodAndBeverage table (name varchar(100));
  INSERT @listOfFoodAndBeverage(name) values ('Alcohol'),('Appetizer'),('Bar'),('Beer'),('Beverage NA'),('Birthday Food (Pizza)'),
                                             ('Birthday N/A Beverage'),('Bottled Water'),('Burgers'),('Desserts'),('Draft Beer'),
											 ('Event Food'),('Food'),('Foods'),('Food & NA Beverage'),('Events N/A Beverage'),('F&B'),
											 ('Food & N/A Beverage'),('Group Food (Pizza)'),('Group N/A Beverage'),('Kids Menu'),
											 ('Liquor'),('NA Beverage'),('N/A Beverage'),('Pizza'),('Recipes'),('Salads'),('Sandwiches'),
											 ('Season Pass Drink'),('Season Pass Popcorn'),('Sides'),('Wine');

	

 
 --######## CREATING TEMP TABLE TO COMBINE SACOA/CENTER EGDE/EMBED ########
 
 IF OBJECT_ID('tempdb..#TEMP_FEG_REVENUE') IS NOT NULL DROP TABLE #TEMP_FEG_REVENUE

SELECT  FEG_REVENUE.* 
  INTO #TEMP_FEG_REVENUE
  FROM (

  SELECT X.Store_ID
       ,X.Business_Date
	   ,SUM(X.AMOUNT) AMOUNT
  FROM
(SELECT store_id
        ,Business_Date
		,SUM(Total) AMOUNT
   FROM [Feg_Etl].[dbo].[sacoa_sales_payments_master]
   WHERE Store_ID not in (2039, 2040, 2041, 2048, 7001)
  group by Store_ID, Business_Date

  UNION ALL

  SELECT SS.[StoreId] as Store_ID
		,SS.[Business_Date]
		,SUM([AmountSold] - [AmountReturned]) AMOUNT
   FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SS
   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] C
     ON SS.CategoryId = C.CategoryId
   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] p
	 ON P.CategoryId = C.ParentCategoryId
  WHERE p.Name not in (select name from @listOfFoodAndBeverage)
  GROUP BY SS.StoreId, SS.Business_Date

  UNION ALL

 SELECT SS.[StoreId] as Store_ID
		,SS.[Business_Date]
		,SUM([AmountSold] - [AmountReturned]) AMOUNT
   FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SS
   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] C
     ON SS.CategoryId = C.CategoryId
   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] p
	 ON P.CategoryId = C.ParentCategoryId
  WHERE p.Name in (select name from @listOfFoodAndBeverage)
  GROUP BY SS.StoreId, SS.Business_Date

  UNION ALL

 --SELECT (SELECT store_number 
	--       FROM [Feg_Etl].[dbo].Master_Store_Information MSI 
	--	  WHERE MSI.Embed_Number = CR.store_id  ) Store_id
 --       ,[Business_Date]
	--    ,SUM([total_received]) AMOUNT
 -- FROM [Feg_Etl].[dbo].[embed_computer_receipts_report_result_master] CR
 -- group by store_id, Business_Date

 SELECT (SELECT store_number 
	       FROM [Feg_Etl].[dbo].Master_Store_Information MSI 
		  WHERE MSI.Embed_Number = CR.store_id  ) Store_id
        ,[Business_Date]
	    ,SUM(cr.total_gross_sales + cr.coupon_used) AMOUNT
  FROM [Feg_Etl].[dbo].embed_card_product_sales_report_result_master CR
  group by store_id, Business_Date
  
  
  ) X

  GROUP BY X.Store_ID, X.Business_Date
		 
		

	) FEG_REVENUE


    --################### LOCATION SALES ###################

IF OBJECT_ID('tempdb..#TEMP_LOCATION_SALES') IS NOT NULL DROP TABLE #TEMP_LOCATION_SALES

SELECT X.* 
  INTO #TEMP_LOCATION_SALES
  FROM (
         SELECT CYS.Store_ID
		        ,'Yesterday' [Period]
				,SUM(CYS.Amount) CY_Amount
				,(SELECT SUM(PYS.Amount) 
		            FROM #TEMP_FEG_REVENUE  PYS
				   WHERE PYS.Store_ID = CYS.Store_ID
				     AND PYS.Business_Date = @PY_DayOfWeek) PY_Amount
				,(SELECT SUM(CYNS.Amount) 
				    FROM #TEMP_FEG_REVENUE CYNS
				   WHERE CYNS.Business_Date = @CY_yesterday
				     AND Store_ID in (select storeId from @listOfLocations )) National_Sales_CY
			    , 1 Periodo_Sort
		   FROM #TEMP_FEG_REVENUE  CYS
		  WHERE Store_ID in (select storeId from @listOfLocations )
		    AND CYS.Business_Date = @CY_yesterday 
          GROUP BY CYS.Store_ID, CYS.Business_Date

          UNION ALL

          -- #### WEEK ###

          SELECT CYS.Store_ID
		         ,'WTD' [Period]
				 ,SUM(CYS.Amount) CY_Amount
				 ,(SELECT SUM(PYS.Amount) 
				     FROM #TEMP_FEG_REVENUE PYS
					WHERE PYS.Store_ID = CYS.Store_ID
					  AND PYS.Business_Date between @PY_StartOfWeek and @PY_DayOfWeek) PY_Amount
			     ,(SELECT SUM(CYNS.Amount) 
				     FROM #TEMP_FEG_REVENUE  CYNS
					WHERE CYNS.Business_Date between @CY_StartOfWeek AND @CY_yesterday
					  AND Store_ID in (select storeId from @listOfLocations )) National_Sales_CY
				 , 2 Periodo_Sort
		    FROM #TEMP_FEG_REVENUE CYS
		   WHERE Store_ID in (select storeId from @listOfLocations )
		     AND CYS.Business_Date between @CY_StartOfWeek AND @CY_yesterday
		   GROUP BY CYS.Store_ID, DATEPART(WEEK,CYS.Business_Date)

           UNION ALL

		   --### MONTH ###

		   SELECT CYS.Store_ID
		          ,'MTD' [Period]
				  ,SUM(CYS.Amount) CY_Amount
				  ,(SELECT SUM(PYS.Amount) 
				      FROM #TEMP_FEG_REVENUE PYS
					 WHERE PYS.Store_ID = CYS.Store_ID
					   AND PYS.Business_Date between @PY_StartOfMonth AND @PY_yesterday) PY_Amount
				  ,(SELECT SUM(CYNS.Amount) 
				      FROM #TEMP_FEG_REVENUE CYNS
					 WHERE CYNS.Business_Date between @CY_StartOfMonth AND @CY_yesterday
					   AND Store_ID in (select storeId from @listOfLocations )) National_Sales_CY
				  , 3 Periodo_Sort
		     FROM #TEMP_FEG_REVENUE CYS
			WHERE Store_ID in (select storeId from @listOfLocations )
			  AND CYS.Business_Date between @CY_StartOfMonth and @CY_yesterday
			GROUP BY CYS.Store_ID, DATEPART(MONTH,CYS.Business_Date)

            UNION ALL

		    --### YEAR ###

		    SELECT CYS.Store_ID
			       ,'YTD' [Period]
				   ,SUM(CYS.Amount) CY_Amount
				   ,(SELECT SUM(PYS.Amount) 
				       FROM #TEMP_FEG_REVENUE PYS
					  WHERE PYS.Store_ID = CYS.Store_ID
					    AND PYS.Business_Date between  @PY_StartOfYear AND @PY_yesterday
	                 ) PY_Amount
				   ,(SELECT SUM(CYNS.Amount) 
				       FROM #TEMP_FEG_REVENUE  CYNS
					  WHERE CYNS.Business_Date between @CY_StartOfYear AND @CY_yesterday
					    AND Store_ID in (select storeId from @listOfLocations )) National_Sales_CY
				   , 4 Periodo_Sort
			  FROM #TEMP_FEG_REVENUE  CYS
			 WHERE Store_ID in (select storeId from @listOfLocations )
			   AND CYS.Business_Date between @CY_StartOfYear AND @CY_yesterday
			 GROUP BY CYS.Store_ID, DATEPART(YEAR,CYS.Business_Date) 
			 
			 ) X


 TRUNCATE TABLE [Feg_Reporting].[dbo].[Location_Sales]
 
   INSERT INTO    [Feg_Reporting].[dbo].[Location_Sales]
   SELECT X.*
     FROM #TEMP_LOCATION_SALES AS X


	  --######## CREATING TEMP TABLE TO COMBINE SACOA/CENTER EGDE/ ########
 
 IF OBJECT_ID('tempdb..#TEMP_FEG_COMBINED') IS NOT NULL DROP TABLE #TEMP_FEG_COMBINED

SELECT FEG_COMBINED.* 
  INTO #TEMP_FEG_COMBINED
  FROM (
         SELECT SS.[StoreId] as Store_ID
	            , 'Food, Beverage & Liquor' as GroupName
				, 'Food, Beverage & Liquor' as GameName
				--,SUM([AmountSold] - [AmountReturned] -[DiscountAmountApplied] ) Amount
				,SUM([AmountSold]  - [AmountReturned]) Amount
				,SS.[Business_Date]
				,'Center Edge' Source
				, 0 AS PayOut_Percentage
				, 0 MoneyOUT
				, 0 [Collection]
				
		   FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SS
		   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] C
		     ON SS.CategoryId = C.CategoryId
		   JOIN [Feg_Etl].[dbo].[center_edge_categories_master] p
		     ON P.CategoryId = C.ParentCategoryId
          WHERE p.Name in (select name from @listOfFoodAndBeverage)

		  GROUP BY SS.StoreId, SS.Business_Date

		  UNION ALL
		 
		  SELECT (SELECT store_number 
	                FROM [Feg_Etl].[dbo].Master_Store_Information MSI 
		           WHERE MSI.Embed_Number = EG.store_id  ) Store_id
                 , CASE WHEN EG.[game_category] = '0 Attraction' THEN 'Attraction'
	                    WHEN EG.[game_category] = '1 Video' THEN 'Video'
			            WHEN EG.[game_category] = '2 Redemption' THEN 'Redemption'
			            WHEN EG.[game_category] = '3 Coin/Token Rolldown' THEN 'Coin/Token Rolldown'
			            WHEN EG.[game_category] = '4 Merchandise' THEN 'Merchandise'
			            ELSE EG.[game_category]
		            END  GroupName
                  ,EG.[game_description] GameName
                  ,EG.[value_of_game_play] Amount
                  ,EG.[Business_Date]
	              ,'Embed' Source
                  ,EG.[percent_ticket_payout] * 100 PayoutPercentage
				  ,EG.paper_tickets_issued + eg.e_tickets_issued MoneyOUT
				  ,EG.card_value + coin_Value  [Collection]
             FROM [Feg_Etl].[dbo].[embed_game_activity_report_result_master] EG

          UNION ALL

		  SELECT 
				   P.[Store_ID] as Store_ID
				  ,[ProductGroup] as GroupName
				  ,[ProductName] as GameName
				 -- ,([Total] - Tax) Amount
				  ,[Total] Amount
				  ,Business_Date
				  ,'Sacoa' Source
				  , 0 AS PayOut_Percentage
				  , 0 MoneyOUT
				  , 0 [Collection]

            FROM [Feg_Etl].[dbo].[sacoa_sales_products_master] AS P 
			where Store_ID in (2042,2044)
			--and ProductGroup in ('Food','Beverages','Dessert')
			and ProductGroup in ('Food','NA Beverage','NA Beverages','Dessert')

		  UNION ALL

	      SELECT ZED.* FROM (
                              SELECT GA.Store_ID as Store_ID
		                             , CASE WHEN GA.GroupName = 'Photo Booth-Instant Win' THEN 'Instant Win'
				                            ELSE GA.GroupName
				                            END as GroupName
				                     ,GA.GameName as GameName
				                     ,case when (select top(1) x.[group] from [Feg_Reporting].[dbo].[vw_group_names] x where x.GroupName = ga.GroupName) = 'Attraction'
									       then SUM(GA.[Collection] + (GA.Passports * GA.AvgPrice)) 
										   else SUM(GA.[Collection]) 
										   end Amount
				                     ,GA.Business_Date
				                     ,'Sacoa' Source
				                     ,SUM(GA.PayOut_Percentage) PayOut_Percentage
									 ,SUM(GA.MoneyOUT) MoneyOUT
									 ,SUM(GA.Collection) [Collection]
		                        FROM [Feg_Etl].[dbo].[sacoa_games_activity_master]  GA
		                       GROUP BY GA.Store_ID, GA.GroupName, GA.GameName, GA.Business_Date

		                       UNION ALL

                              select a.storeId
	                                 ,a.GroupName
		                             ,'None'
		                             ,0
		                             ,@CY_yesterday
		                             ,'Sample'
		                             ,0
									 ,0
									 ,0
                                FROM (select GroupName, storeId 
								        from #listOfGroupNames 
	                                   right Join @listOfLocations on 1=1) A

		                    ) ZED

	) FEG_COMBINED

     --################### LOCATION EQUIPMENT ###################
IF OBJECT_ID('tempdb..#TEMP_LOCATION_EQUIPMENT') IS NOT NULL DROP TABLE #TEMP_LOCATION_EQUIPMENT

SELECT X.* 
  INTO #TEMP_LOCATION_EQUIPMENT 
  FROM ( SELECT CYGA.Store_ID
                , CYGA.GroupName
				, CYGA.GameName
				, SUM(CYGA.Amount) CY_Yest_Collection

				, (SELECT SUM(CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
					WHERE CEYGA.Store_ID = CYGA.Store_Id
					  AND CEYGA.Business_Date = @CY_EreYesterday
					  AND CEYGA.GroupName = CYGA.GroupName
					  AND CEYGA.GameName = CYGA.GameName
					GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) CY_EreYest_Collection

	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date = @CY_PriorSaturday
			          AND CEYGA.GroupName = CYGA.GroupName
			          AND CEYGA.GameName = CYGA.GameName
			        GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) CY_PriorSaturday_Collection

	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date between @CY_StartOfWeek AND @CY_yesterday
			          AND CEYGA.GroupName = CYGA.GroupName
			          AND CEYGA.GameName = CYGA.GameName
			        GROUP BY CEYGA.Store_ID, CEYGA.GroupName,  CEYGA.GameName ) CY_Weekly_Collection
	   

				, ( SELECT AVG(T.AMT) FROM (SELECT SUM (CEYGA.Amount)  AMT
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID in (select storeId from @listOfLocations )
		              AND CEYGA.Business_Date between @CY_StartOfWeek AND @CY_yesterday
			          AND CEYGA.GroupName = CYGA.GroupName
			          AND CEYGA.GameName = CYGA.GameName
			        GROUP BY CEYGA.GroupName,  CEYGA.GameName, CEYGA.Store_ID ) T ) CY_National_Average_Collection

		        , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date = @CY_yesterday
					  AND CEYGA.GroupName NOT IN( 'Food, Beverage & Liquor')
			        GROUP BY CEYGA.Store_ID ) CY_Yest_Total

		        , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date = @CY_EreYesterday
					  AND CEYGA.GroupName NOT IN( 'Food, Beverage & Liquor')
			        GROUP BY CEYGA.Store_ID ) CY_PriorDay_Total

		        , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date = @CY_PriorSaturday
					  AND CEYGA.GroupName NOT IN( 'Food, Beverage & Liquor')
			        GROUP BY CEYGA.Store_ID ) CY_PriorSaturday_Total

	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE (CEYGA.Store_ID = CYGA.Store_Id)
		              AND CEYGA.Business_Date between @CY_StartOfWeek AND @CY_yesterday
					  AND CEYGA.GroupName NOT IN( 'Food, Beverage & Liquor')
			        GROUP BY CEYGA.Store_ID ) CY_Weekly_Total

	            , ((SELECT SUM (CEYGA.PayOut_Percentage) 
	                  FROM #TEMP_FEG_COMBINED CEYGA
	                 WHERE CEYGA.Store_ID = CYGA.Store_Id
		           --    AND CEYGA.Business_Date = @CY_EreYesterday
					   AND CEYGA.Business_Date =  @CY_yesterday
			           AND CEYGA.GroupName = CYGA.GroupName
			           AND CEYGA.GameName = CYGA.GameName
			         GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) * 0.01) CY_Yest_PayOut
	 
	            , ( (SELECT  CASE WHEN SUM(CEYGA.Collection) = 0 THEN 0
		                          ELSE SUM(CEYGA.MoneyOUT) / SUM(CEYGA.Collection)
			                 END
	                   FROM #TEMP_FEG_COMBINED CEYGA
	                  WHERE CEYGA.Store_ID = CYGA.Store_Id
		                AND CEYGA.Business_Date between @CY_StartOfWeek AND @CY_yesterday
			            AND CEYGA.GroupName = CYGA.GroupName
			            AND CEYGA.GameName = CYGA.GameName
			          GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) ) CY_Weekly_PayOut
	 
	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date = @PY_DayOfWeek
			          AND CEYGA.GroupName = CYGA.GroupName
			          AND CEYGA.GameName = CYGA.GameName
			        GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) PY_Yest_Collection

	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date between @CY_StartOfMonth AND @CY_yesterday
			        GROUP BY CEYGA.Store_ID ) CY_Month_Total

	            , (SELECT SUM (CEYGA.Amount) 
	                 FROM #TEMP_FEG_COMBINED CEYGA
	                WHERE CEYGA.Store_ID = CYGA.Store_Id
		              AND CEYGA.Business_Date between @CY_StartOfYear AND @CY_yesterday
			        GROUP BY CEYGA.Store_ID ) CY_Year_Total
	 
	            , @CY_yesterday CY_Yesterday

				,(SELECT SUM (CEYGA.Amount) 
	                FROM #TEMP_FEG_COMBINED CEYGA
	               WHERE CEYGA.Store_ID = CYGA.Store_Id
		             AND CEYGA.Business_Date between @CY_StartOfWeek AND @CY_yesterday
			         AND CEYGA.GroupName = CYGA.GroupName
					 --inserted next line
					 AND CEYGA.GameName = CYGA.GameName
			       GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName  ) CY_Weekly_G_Col
				
				,(SELECT SUM (CEYGA.Amount) 
	                FROM #TEMP_FEG_COMBINED CEYGA
	               WHERE CEYGA.Store_ID = CYGA.Store_Id
		             AND CEYGA.Business_Date between @CY_StartOfMonth AND @CY_yesterday
			         AND CEYGA.GroupName = CYGA.GroupName
					 AND CEYGA.GameName = CYGA.GameName
			       GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) CY_MTD_G_Col

				,(SELECT SUM (CEYGA.Amount) 
	                FROM #TEMP_FEG_COMBINED CEYGA
	               WHERE CEYGA.Store_ID = CYGA.Store_Id
   	             AND CEYGA.Business_Date between @CY_StartOfYear AND @CY_yesterday
			         AND CEYGA.GroupName = CYGA.GroupName
					 AND CEYGA.GameName = CYGA.GameName
			       GROUP BY CEYGA.Store_ID, CEYGA.GroupName, CEYGA.GameName ) CY_YTD_G_Col

             FROM #TEMP_FEG_COMBINED CYGA
			WHERE CYGA.Store_ID in (select storeId from @listOfLocations )
			  AND CYGA.Business_Date = @CY_yesterday

 GROUP BY CYGA.Store_ID, CYGA.GroupName, CYGA.GameName ) X


TRUNCATE TABLE [Feg_Reporting].[dbo].[Location_Equipment]
INSERT INTO    [Feg_Reporting].[dbo].[Location_Equipment]

 SELECT X.*
   FROM #TEMP_LOCATION_EQUIPMENT AS X





END
