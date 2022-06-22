USE [Feg_Reporting]
GO
/****** Object:  StoredProcedure [dbo].[prc_load_center_edge_daily_combined_report]    Script Date: 6/22/2022 8:12:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Description:	Load Reporting Tables
-- =============================================


ALTER PROCEDURE [dbo].[prc_load_center_edge_daily_combined_report]

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


/***  START OF CENTER_EDGE_LABOR_COST_ANALYSIS  ***/

IF OBJECT_ID('tempdb..#TEMP_LABOR_COST_ANALYSIS') IS NOT NULL DROP TABLE #TEMP_LABOR_COST_ANALYSIS

SELECT	[Store_Id]
		,[Work_Date]
		,[Week_Day]
		,[Hour]
		--, dateadd(hour, [Hour], dateadd(minute,0, dateadd(second, 0, dateadd(millisecond, 0 , cast('00:00:00' as time(2)))))) [Hour]
		,[Labor_cost]
		,[Hours_Worked]
		,[Sales]
		,[HourAMPM]
      
	  INTO #TEMP_LABOR_COST_ANALYSIS
	  
  FROM (
		  SELECT 
				LH.StoreId Store_Id
				,LH.WorkDate Work_Date
				,DATEPART(dw, LH.WorkDate) Week_Day
				,LH.Hour
				,ROUND(SUM(LH.EffectiveHourlyLaborCost),2) Labor_cost
				,ROUND(SUM(LH.LaborHours),2) Hours_Worked
				,COALESCE(ROUND(( SELECT SUM(SS.AmountSold - SS.amountreturned)
						   FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SS 
						  WHERE SS.StoreId = LH.StoreId
							AND SS.Business_Date = LH.WorkDate
							AND SS.HourNo = LH.Hour
					   ),2), 0) Sales
				,CAST((cast(lh.Hour as VARCHAR) +':00') AS time ) HourAMPM
			FROM [Feg_Etl].[dbo].[center_edge_laborHourly_master] LH
		GROUP BY lh.WorkDate
					,LH.Hour
					,LH.StoreId
		) T

		---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_labor_cost_analysis]
INSERT INTO    [Feg_Reporting].dbo.[center_edge_labor_cost_analysis]

 SELECT X.*
   FROM #TEMP_LABOR_COST_ANALYSIS AS X



/***  END OF  CENTER_EDGE_LABOR_COST_ANALYSIS  ***/

/***  START OF CENTER_EDGE_HOURLY_PAYROLL_WORKED   ***/

IF OBJECT_ID('tempdb..#TEMP_HOURLY_PAYROLL_WORKED') IS NOT NULL DROP TABLE #TEMP_HOURLY_PAYROLL_WORKED

SELECT	[StoreID]
		,[Department]
		,[Business_Date]
		,[Employee_Number]
		,[Employee_Name]
		,[Schedule_Hours]
		,[Regular_Hours]
		,[Overtime_Hours]
		,[Regular_Pay]
		,[Overtime_Pay]
		,[Schedule_Pay]
      
	  INTO #TEMP_HOURLY_PAYROLL_WORKED
	  
  FROM ( 
		  SELECT C.* 
		   FROM ( 
					 SELECT
				CASE 
					WHEN LM.StoreId IS NULL THEN ESM.StoreId
					ELSE LM.StoreId
					END StoreId
				,CASE
					WHEN LM.Department  IS NULL THEN ESM.Department
					ELSE LM.Department 
					END as Department
				,CASE
					WHEN LM.Business_Date IS NULL THEN ESM.Business_Date
					ELSE LM.Business_Date
					END Business_Date
				,CASE 
					WHEN EM.EmployeeNumber IS NULL THEN ESM.EmployeeNumber
					ELSE EM.EmployeeNumber
					END Employee_Number
				,CASE 
					WHEN EM.LastName IS NULL THEN ESM.Employee_Name
					ELSE (EM.LastName + ', ' + EM.FirstName ) 
					END Employee_Name
				,CASE 
					WHEN ESM.ScheduledHours IS NULL THEN 0
					ELSE ESM.ScheduledHours
					END Schedule_Hours
				,(SELECT SUM (R1.RegularHours) 
				    from [Feg_Etl].[dbo].[center_edge_labor_master] R1
					WHERE R1.StoreId = LM.StoreId
					  AND R1.Department = LM.Department
					  AND R1.Business_Date = LM.Business_Date
					  AND R1.EmpId = LM.EmpId
					GROUP BY R1.StoreId, R1.Department, R1.Business_Date, R1.EmpId
				  )Regular_Hours
				,(SELECT SUM (R1.OvertimeHours) 
				    from [Feg_Etl].[dbo].[center_edge_labor_master] R1
					WHERE R1.StoreId = LM.StoreId
					  AND R1.Department = LM.Department
					  AND R1.Business_Date = LM.Business_Date
					  AND R1.EmpId = LM.EmpId
					GROUP BY R1.StoreId, R1.Department, R1.Business_Date, R1.EmpId
				  )Overtime_Hours
				 ,(SELECT SUM (R1.RegularPay) 
				    from [Feg_Etl].[dbo].[center_edge_labor_master] R1
					WHERE R1.StoreId = LM.StoreId
					  AND R1.Department = LM.Department
					  AND R1.Business_Date = LM.Business_Date
					  AND R1.EmpId = LM.EmpId
					GROUP BY R1.StoreId, R1.Department, R1.Business_Date, R1.EmpId
				  )Regular_Pay
				 ,(SELECT SUM (R1.OvertimePay) 
				    from [Feg_Etl].[dbo].[center_edge_labor_master] R1
					WHERE R1.StoreId = LM.StoreId
					  AND R1.Department = LM.Department
					  AND R1.Business_Date = LM.Business_Date
					  AND R1.EmpId = LM.EmpId
					GROUP BY R1.StoreId, R1.Department, R1.Business_Date, R1.EmpId
				  )Overtime_Pay
				  ,CASE 
					WHEN ESM.PayRate IS NULL THEN ESM.DefaultPayRate * ESM.ScheduledHours
					ELSE ESM.PayRate * ESM.ScheduledHours
					END Schedule_Pay
				
			FROM  (
					select distinct StoreId, EmpId, Department, Business_Date 
					  from [Feg_Etl].[dbo].[center_edge_labor_master]) LM
		

			JOIN [Feg_Etl].[dbo].[center_edge_employees_master] EM 
			  ON LM.EmpId = EM.EmployeeId
			 AND LM.StoreId = EM.StoreId
       FULL JOIN (	 SELECT	DISTINCT X.*
							,Y.EmployeeNumber
							,(Y.LastName + ', ' + Y.FirstName ) Employee_Name 
				       FROM [Feg_Etl].[dbo].[center_edge_employeeSchedule_master] X
				     JOIN [Feg_Etl].[dbo].[center_edge_employees_master] Y
				       ON  x.EmpId = y.EmployeeId
				      AND x.StoreId = y.StoreId
				 ) ESM
			  ON (LM.EmpId = ESM.EmpId)
			 AND (LM.Department = ESM.Department)
			 AND (LM.Business_Date = ESM.Business_Date)
			 AND (LM.StoreId = ESM.StoreId) 
		   ) C
        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_hourly_payroll_worked]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_hourly_payroll_worked]

 SELECT X.*
   FROM #TEMP_HOURLY_PAYROLL_WORKED AS X



/***  END OF CENTER_EDGE_HOURLY_PAYROLL_WORKED   ***/

/***  START OF CENTER_EDGE_CASHIER_OVER_SHORT  ***/

IF OBJECT_ID('tempdb..#TEMP_CASHIER_OVER_SHORT') IS NOT NULL DROP TABLE #TEMP_CASHIER_OVER_SHORT

SELECT	
		 [Store_Id]
		,[Employee_Name]
		,[Till_Over_Short_Amount]
		,[Manual_Redemption_Tickets]
		,[Business_Date]
		,[Reason]
		,[MgrEmpNo]
		,[Department]
		,[Position]
		,[Revenue_Center]
		
      
	  INTO #TEMP_CASHIER_OVER_SHORT
	  
  FROM (	  SELECT
					 EH.StoreId  Store_Id
					,(E.LastName + ', ' + E.FirstName) Employee_Name
					,[TillOverShortAmount] Till_Over_Short_Amount
					,[ManualRedemptionTickets] Manual_Redemption_Tickets
					,EH.Business_Date
					,[MgrEmpNo]
					,COALESCE( (SELECT TOP(1) Department FROM [Feg_Etl].[dbo].[center_edge_labor_master] X 
						WHERE X.StoreId = EH.StoreId AND X.EmpId = EH.EmpId AND X.Business_Date = EH.Business_Date), 'Entire Site')Department
					,COALESCE( (SELECT TOP(1) Position FROM [Feg_Etl].[dbo].[center_edge_labor_master] X 
						WHERE X.StoreId = EH.StoreId AND X.EmpId = EH.EmpId AND X.Business_Date = EH.Business_Date), 'Kiosk')Position
					,COALESCE( (SELECT TOP(1) RevenueCenter FROM [Feg_Etl].[dbo].[center_edge_labor_master] X 
						WHERE X.StoreId = EH.StoreId AND X.EmpId = EH.EmpId AND X.Business_Date = EH.Business_Date), 'Entire Site')Revenue_Center
					, CASE
							WHEN EH.OverShortReason IS NULL THEN 'Balanced'
							ELSE EH.OverShortReason
							END as Reason
			  FROM [Feg_Etl].[dbo].[center_edge_employeeHistory_master] EH
			  JOIN [Feg_Etl].[dbo].[center_edge_employees_master] E
				ON EH.EmpId = e.EmployeeId
			   AND EH.StoreId = E.StoreId

        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_cashier_over_short]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_cashier_over_short]

 SELECT X.*
   FROM #TEMP_CASHIER_OVER_SHORT AS X



/***  END OF CENTER_EDGE_CASHIER_OVER_SHORT  ***/

/***  START OF CENTER_EDGE_HOURS_WORKED_WARNING  ***/

--GETTING THE WORKING WEEK
DECLARE @WEEKSTART DATE,
		@WEEKEND DATE,
		@DAY DATE
    SET @DAY = GETDATE();
	SET DATEFIRST 1;
	SET @WEEKEND = CONVERT(varchar(50), (DATEADD(dd, @@DATEFIRST - DATEPART(dw, @DAY) + 6, @DAY)), 101);
	SET @WEEKSTART = CONVERT(varchar(50), (DATEADD(dd, @@DATEFIRST - DATEPART(dw, @DAY), @DAY)), 101);

IF OBJECT_ID('tempdb..#TEMP_HOURS_WORKED_WARNING') IS NOT NULL DROP TABLE #TEMP_HOURS_WORKED_WARNING

SELECT	[Store_Id]
		,[Business_date]
		,[Total_Hours]
		,[Type_Hours]
		,[Employee_Name]
		,[Week]
		,[Year]
		,[over_time]

  INTO #TEMP_HOURS_WORKED_WARNING
/*** OLD LOGIC ***/  
  --FROM ( 
		--	SELECT x.storeId Store_Id
		--		   , x.business_date Business_date
		--		   , CASE WHEN x.business_date < @day THEN SUM(x.total_hours)
		--			      ELSE SUM(x.scheduled_hours)
		--				  END Total_Hours
		--		   , CASE WHEN x.business_date < @day THEN 'Worked'
		--				  ELSE 'Scheduled' 
		--				  END Type_Hours
		--		   , (em.LastName +', '+ em.FirstName) Employee_Name
		--		   , DATEPART(wk, x.business_date) [Week]
		--		   , DATEPART(YEAR, x.business_date) [Year]

		--	 FROM (SELECT [StoreId] storeId
		--				  ,[Business_Date] business_date
		--				  ,[EmpId] empId
		--				  ,SUM(sm.ScheduledHours) scheduled_hours
		--				  ,0 total_hours
		--			FROM [Feg_Etl].[dbo].[center_edge_employeeSchedule_master] sm
		--           GROUP BY sm.StoreId, sm.Business_Date,  sm.EmpId

		--           UNION ALL

		--		  SELECT [StoreId] storeId
		--				 ,[Business_Date] business_date
		--				 ,[EmpId] empId
		--				 ,0 scheduled_hours
		--				 ,sum (lm.RegularHours + lm.OvertimeHours) total_hours
		--			FROM [Feg_Etl].[dbo].[center_edge_labor_master] lm
		--           GROUP BY lm.StoreId, lm.Business_Date,  lm.EmpId
		--		   ) x
		--	JOIN [Feg_Etl].[dbo].[center_edge_employees_master] em
		--	  ON x.empId = em.EmployeeId 
		--	 AND x.storeId = em.StoreId
	       
		----   WHERE x.business_date BETWEEN @WEEKSTART AND @WEEKEND
		--   GROUP BY x.storeId, x.business_date, x.empId, em.LastName, em.FirstName

  --      ) T

/*** NEW LOGIC ***/

  FROM ( 
			SELECT x.storeId Store_Id
				   , x.business_date Business_date
				   , CASE WHEN x.business_date < @day THEN SUM(x.total_hours)
					      ELSE SUM(x.scheduled_hours)
						  END Total_Hours
				   , CASE WHEN x.business_date < @day THEN 'Worked'
						  ELSE 'Scheduled' 
						  END Type_Hours
				   , (em.LastName +', '+ em.FirstName) Employee_Name
				   , DATEPART(wk, x.business_date) [Week]
				   , DATEPART(YEAR, x.business_date) [Year]
				   , sum(x.over_time) over_time

			 FROM (SELECT [StoreId] storeId
						  ,[Business_Date] business_date
						  ,[EmpId] empId
						  ,SUM(sm.ScheduledHours) scheduled_hours
						  ,0 total_hours
						  ,0 over_time
					FROM [Feg_Etl].[dbo].[center_edge_employeeSchedule_master] sm
					WHERE sm.business_date BETWEEN @WEEKSTART AND @WEEKEND
		           GROUP BY sm.StoreId, sm.Business_Date,  sm.EmpId

		           UNION ALL

				  SELECT [StoreId] storeId
						 ,[Business_Date] business_date
						 ,[EmpId] empId
						 ,0 scheduled_hours
						 ,sum (lm.RegularHours + lm.OvertimeHours) total_hours
						 ,sum(lm.OvertimeHours) over_time
					FROM [Feg_Etl].[dbo].[center_edge_labor_master] lm
				--	where lm.OvertimeHours > 0
		           GROUP BY lm.StoreId, lm.Business_Date,  lm.EmpId
				   ) x
			JOIN [Feg_Etl].[dbo].[center_edge_employees_master] em
			  ON x.empId = em.EmployeeId 
			 AND x.storeId = em.StoreId
	       
		--   WHERE x.business_date BETWEEN @WEEKSTART AND @WEEKEND
		   GROUP BY x.storeId, x.business_date, x.empId, em.LastName, em.FirstName

        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_hours_worked_warning]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_hours_worked_warning]

 --SELECT X.*
 --  FROM #TEMP_HOURS_WORKED_WARNING AS X

 SELECT TMP1.Store_Id
	   ,TMP1.Business_date
	   ,TMP1.Total_Hours
	   ,TMP1.Type_Hours
	   ,TMP1.Employee_Name
	   ,TMP1.[Week]
	   ,TMP1.[Year]
	   ,(SELECT SUM(TMPS.Total_Hours)
	       FROM #TEMP_HOURS_WORKED_WARNING TMPS
		   WHERE TMPS.Store_Id = TMP1.Store_Id
		     AND TMPS.Employee_Name = TMP1.Employee_Name
			 AND TMPS.[Week] = TMP1.[Week]
			 AND TMPS.[Year] = TMP1.[Year]
	    ) Total_Hours_Week
	   ,TMP1.[over_time]
  FROM #TEMP_HOURS_WORKED_WARNING TMP1



/***  END OF CENTER_EDGE_HOURS_WORKED_WARNING  ***/

/***  START OF CENTER_EDGE_HOURLY_SALES ***/


IF OBJECT_ID('tempdb..#TEMP_HOURLY_SALES') IS NOT NULL DROP TABLE #TEMP_HOURLY_SALES

SELECT	[Store_Id]
		,[Business_date]
		,[HourNo]
		,[Division]
		,[Product_Number]
		,[Amount_Sold]
		,[Quantity_Sold]
		,[Amount_Returned]
		,[Quantity_Returned]
		,[Redemption_Tickets_Redeemed]
		,[Discount_Amount_Applied]
		,[Revenue_Center]
		,[Category]
		,[Category_Parent]
		,[Product_name]

  INTO #TEMP_HOURLY_SALES
	  
  FROM ( 
			SELECT  SSM.[StoreId] Store_Id
			       ,SSM.[Business_Date] Business_Date
				   ,SSM.[HourNo] HourNo
				   ,SSM.[Division] Division
				   ,ssm.ProductId
				   ,(Select TOP 1 x.ItemNumber from [Feg_Etl].[dbo].[center_edge_products_master] x
				   where x.ProductId = pm.ProductId and x.StoreId = pm.StoreId) Product_Number
				   ,[AmountSold] Amount_Sold
				   ,[QuantitySold] Quantity_Sold
				   ,[AmountReturned] Amount_Returned
				   ,[QuantityReturned] Quantity_Returned
				   ,[RedemptionTicketsRedeemed] Redemption_Tickets_Redeemed
				   ,[DiscountAmountApplied] Discount_Amount_Applied
				   ,[RevenueCenter] Revenue_Center
				   ,CM.[Name] Category
				   ,CMP.[Name] Category_Parent
				   ,PM.[Name] Product_name
			  FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SSM
			  JOIN (select distinct x.ProductId, x.Name, x.StoreId, x.CategoryId from [Feg_Etl].[dbo].[center_edge_products_master] x) PM
			    ON SSM.ProductId = PM.ProductId
			   AND SSM.StoreId = PM.StoreId
	     LEFT JOIN [Feg_Etl].[dbo].[center_edge_categories_master] CM
		        ON SSM.CategoryId = CM.CategoryId
			   AND SSM.StoreId = CM.StoreId
	     LEFT JOIN [Feg_Etl].[dbo].[center_edge_categories_master] CMP
		        ON CMP.CategoryId = CM.ParentCategoryId
			   AND CMP.StoreId = CM.StoreId

        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_hourly_sales]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_hourly_sales]

 SELECT X.*
   FROM #TEMP_HOURLY_SALES AS X



/***  END OF CENTER_EDGE_HOURLY_SALES  ***/

/***  START OF CENTER_EDGE_RETURNS_OVER_DATA ***/


IF OBJECT_ID('tempdb..#TEMP_RETURNS_OVER_DATA') IS NOT NULL DROP TABLE #TEMP_RETURNS_OVER_DATA

SELECT	[Store_Id]
		,[Business_date]
		,[Receipt_Number]
		,[Hour]
		,[Employee_Name]
		,[Division]
		,[Product]
		,[Quantity]
		,[Amount_Paid]
		,[Refund_Amount]
		,[Authorized_By]
		,[RefNo]
		,[RefundReason]
		,[Category]
		

  INTO #TEMP_RETURNS_OVER_DATA
	  
  FROM ( 
			SELECT DISTINCT  TM.[StoreId] Store_Id
					,TM.[Business_Date] Business_Date
					,[ReceiptNumber] Receipt_Number
					,convert(varchar, TransactionDateTime,8) [Hour]
					,EM.LastName + ', ' + EM.FirstName Employee_Name
					,TM.[Division] Division
					,PM.[Name] Product
					,CASE WHEN [LineQuantitySold] < 0 THEN [LineQuantitySold] * -1 ELSE [LineQuantitySold] END Quantity
					,CASE WHEN [LineAmountPaid] < 0 THEN [LineAmountPaid] * -1 ELSE [LineAmountPaid] END Amount_Paid
					,CASE WHEN [RefundAmount] < 0 THEN [RefundAmount] * -1 ELSE [RefundAmount] END Refund_Amount
					,EMS.LastName + ', ' + EMS.FirstName Authorized_By
					,[RefNo]
					,[RefundReason]
					,CM.Name Category
					,TM.LineNumber
			   FROM [Feg_Etl].[dbo].[center_edge_transactions_master] TM
			   JOIN [Feg_Etl].[dbo].[center_edge_employees_master] EM
			     ON TM.StoreId = EM.StoreId
				AND TM.EmpId = EM.EmployeeId
		  LEFT JOIN [Feg_Etl].[dbo].[center_edge_employees_master] EMS
		         ON TM.StoreId = EMS.StoreId
				AND TM.AuthByEmpNo = EMS.EmployeeId
			   JOIN [Feg_Etl].[dbo].[center_edge_products_master] PM
			     ON PM.ProductId = TM.LineProductId
			    AND PM.StoreId = TM.StoreId
		  LEFT JOIN [Feg_Etl].[dbo].[center_edge_categories_master] CM
			     ON CM.StoreId = PM.StoreId
			    AND CM.CategoryId = PM.CategoryId
			  WHERE (
					--	TM.RefundReceipt = 1 
					-- OR 
					 TM.LineQuantitySold < 0
					)

        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_returns_over_data]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_returns_over_data]

 SELECT X.*
   FROM #TEMP_RETURNS_OVER_DATA AS X



/***  END OF CENTER_EDGE_RETURNS_OVER_DATA  ***/

/***  START OF CENTER_EDGE_END_DAY_BALLANCE ***/


IF OBJECT_ID('tempdb..#TEMP_END_DAY_BALLANCE') IS NOT NULL DROP TABLE #TEMP_END_DAY_BALLANCE

SELECT	[Store_Id]
		,[Business_date]
		,[Tender_Type]
		,[Amount_Debit]
		,[Parent_Category_Number]
		,[Parent_Category_Name]
		,[Category_Number]
		,[Category_Name]
		,[Amount_Credit]
		,[DC]

  INTO #TEMP_END_DAY_BALLANCE
	  
  FROM ( 
		
	        SELECT StoreId Store_Id
			       ,Business_Date
				   ,tendertype Tender_Type
				   ,SUM(Amount) Amount_Debit
				   ,'' Parent_Category_Number
				   ,'' Parent_Category_Name
				   ,'' Category_Number
				   ,'' Category_Name
				   ,'' Amount_Credit
				   ,'Debits' DC
			  FROM [Feg_Etl].[dbo].[center_edge_income_master]
			 WHERE TenderType not in ('Player Card', 'Employee Discount', 'Discounts', 'Gratuity', 'Coupon',
									   'Member Discount', 'Reschedule'	)
		  GROUP BY StoreId, Business_Date, TenderType

             UNION ALL
		    SELECT EM.StoreId Store_Id
			       ,EM.Business_Date
				   ,'Birthday Deposits Redeemed' Tender_Type
				   ,SUM(TM.LineAmountPaid) Amount_Debit
				   ,'' Parent_Category_Number
				   ,'' Parent_Category_Name
				   ,'' Category_Number
				   ,'' Category_Name
				   ,'' Amount_Credit
				   ,'Debits' DC
			  FROM [Feg_Etl].[dbo].[center_edge_events_master] EM
			  JOIN [Feg_Etl].[dbo].[center_edge_transactions_master] TM
			    ON EM.StoreId = TM.StoreId 
			   AND EM.EventRefId = TM.EventRefId
			   AND EM.Business_Date > TM.Business_Date
			    AND EM.Status <> 'Canceled'
			 GROUP BY EM.StoreId, EM.Business_Date, TenderType

			 UNION ALL

			 SELECT x.StoreId Store_Id
				    , x.[Business_Date] Business_Date
					,'IL Sales Tax' Tender_Type
					,(sum(x.TaxAmount) * -1) Amount_Debit
					,'' Parent_Category_Number
					,'' Parent_Category_Name
					,'' Category_Number
					,'' Category_Name
					,'' Amount_Credit 
					,'Debits' DC
			   FROM ( SELECT DISTINCT StoreId
			                          ,ReceiptNumber
									  ,Business_Date
									  ,TaxAmount
								 FROM [Feg_Etl].[dbo].[center_edge_transactions_master] ) x
           GROUP BY StoreId, Business_Date

		   UNION ALL

			 SELECT x.StoreId Store_Id
				    , x.[Business_Date] Business_Date
					,'Gratuity' Tender_Type
					,(sum(x.GratuityAmount) * -1 ) Amount_Debit
					,'' Parent_Category_Number
					,'' Parent_Category_Name
					,'' Category_Number
					,'' Category_Name
					,''  Amount_Credit 
					,'Debits' DC
			   FROM ( SELECT DISTINCT StoreId
			                          ,ReceiptNumber
									  ,Business_Date
									  ,GratuityAmount
								 FROM [Feg_Etl].[dbo].[center_edge_transactions_master] ) x
           GROUP BY StoreId, Business_Date


			 UNION ALL

			SELECT TM.StoreId Store_Id
			       , TM.[Business_Date] Business_Date
				   ,'Birthday Deposits Received' Tender_Type
				   ,(sum(TM.LineAmountPaid) * -1) Amount_Debit
				   ,'' Parent_Category_Number
				   ,'' Parent_Category_Name
				   ,'' Category_Number
				   ,'' Category_Name
				   ,'' Amount_Credit 
				   ,'Debits' DC
			  FROM [Feg_Etl].[dbo].[center_edge_transactions_master] TM
			  JOIN [Feg_Etl].[dbo].[center_edge_events_master] EM
			    ON TM.StoreId = EM.StoreId
			   AND TM.EventRefId = EM.EventRefId
			   AND TM.Business_Date <> EM.Business_Date
			   AND EM.IsBirthdayEvent = 1
          GROUP BY TM.StoreId, TM.Business_Date

			 UNION ALL

		    SELECT 
			       SS.[StoreId] Store_Id
				   ,SS.[Business_Date] Business_Date
				   ,'' Tender_Type
				   ,'' Amount_Debit
				   ,CMP.CategoryNumber Parent_Category_Number
				   ,CMP.Name Parent_Category_Name
				   ,CM.CategoryNumber Category_Number
				   ,CM.Name Category_Name
				   ,SUM([AmountSold] - [AmountReturned]) Amount_Credit
				   ,'Credits' DC
		      FROM [Feg_Etl].[dbo].[center_edge_salesSummary_master] SS
			  JOIN [Feg_Etl].[dbo].[center_edge_categories_master] CM
			    ON SS.StoreId = CM.StoreId
			   AND SS.CategoryId = CM.CategoryId
			  JOIN [Feg_Etl].[dbo].[center_edge_categories_master] CMP
				ON CM.ParentCategoryId = CMP.CategoryId
			   AND CM.StoreId = CMP.StoreId
		     GROUP BY SS.[StoreId] 
			          ,CMP.CategoryNumber
					  ,CMP.Name
					  ,CM.CategoryNumber
					  ,CM.Name
					  ,SS.[Business_Date]

        ) T

---   POPULATING TABLE   ---

TRUNCATE TABLE [Feg_Reporting].[dbo].[center_edge_end_day_ballance]
INSERT INTO    [Feg_Reporting].[dbo].[center_edge_end_day_ballance]

 SELECT X.*
   FROM #TEMP_END_DAY_BALLANCE AS X



/***  END OF CENTER_EDGE_END_DAY_BALLANCE   ***/

END
