/*
===================================================================================================
>> loading the bronze layer <<

this script load the bronze layer from the source(.csv files) to the tables created in the DDL file.
in each time it works it truncates the tables from any already exsisted data and load it with the new data from the sources files using bulk command.

to use it type: EXEC bronze.load_bronze;
====================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @abs_start_time DATETIME, @abs_end_time DATETIME;
	BEGIN TRY
  	SET @abs_start_time = GETDATE();
  
  		PRINT '============================';
  		PRINT 'loading the bronze layer';
  		PRINT '============================';
  
  
  		PRINT '------------------------';
  		PRINT 'loading CRM tables';
  		PRINT '------------------------';
  		
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.crm_cust_info <<';
  		TRUNCATE TABLE bronze.crm_cust_info;
  
  		PRINT '>> loading table: bronze.crm_cust_info <<';
  		BULK INSERT bronze.crm_cust_info 
  		FROM 'D:\education\datawarehouse files\source _crm\cust_info.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=============================================================';
  		PRINT '=============================================================';
  
  
  
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.crm_prd_info <<';
  		TRUNCATE TABLE bronze.crm_prd_info;
  
  		PRINT '>> loading table: bronze.crm_prd_info <<';
  		BULK INSERT bronze.crm_prd_info 
  		FROM 'D:\education\datawarehouse files\source _crm\prd_info.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=============================================================';
  		PRINT '=============================================================';
  
  
  
  
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.crm_sales_details <<';
  		TRUNCATE TABLE bronze.crm_sales_details;
  
  		PRINT '>> loading table: bronze.crm_sales_details <<';
  		BULK INSERT bronze.crm_sales_details 
  		FROM 'D:\education\datawarehouse files\source _crm\sales_details.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=============================================================';
  		PRINT '=============================================================';
  
  
  
  
  
  		
  		PRINT '------------------------';
  		PRINT 'loading ERP tables';
  		PRINT '------------------------';
  
  
  
  
  
  
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.erp_cust_az12 <<';
  		TRUNCATE TABLE bronze.erp_cust_az12 ;
  
  		PRINT '>> loading table: bronze.erp_cust_az12 <<';
  		BULK INSERT bronze.erp_cust_az12 
  		FROM 'D:\education\datawarehouse files\source_erp\cust_az12.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=============================================================';
  		PRINT '=============================================================';
  
  
  
  
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.erp_loc_a101 <<';
  		TRUNCATE TABLE bronze.erp_loc_a101 ;
  
  		PRINT '>> loading table: bronze.erp_loc_a101 <<';
  		BULK INSERT bronze.erp_loc_a101  
  		FROM 'D:\education\datawarehouse files\source_erp\loc_a101.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=============================================================';
  		PRINT '=============================================================';
  
  
  
  
  
  		SET @start_time = GETDATE();
  		PRINT '>> trancating table: bronze.erp_px_cat_g1v2 <<';
  		TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;
  
  		PRINT '>> loading table: bronze.erp_px_cat_g1v2 <<';
  		BULK INSERT bronze.erp_px_cat_g1v2
  		FROM 'D:\education\datawarehouse files\source_erp\px_cat_g1v2.csv'
  		WITH (
  			FIRSTROW = 2,
  			FIELDTERMINATOR = ',',
  			TABLOCK
  		);
  		SET @end_time = GETDATE();
  		PRINT '>> loading time: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
  		
  
  		PRINT '=====================================';
  		PRINT 'loading the bronze layer is completed';
  		PRINT '=====================================';
  
  		PRINT ' ';
  		SET @abs_end_time = GETDATE();
  		PRINT '>> entier loading time: ' + cast(DATEDIFF(second, @abs_start_time, @abs_end_time) AS NVARCHAR) + ' seconds <<';
  		PRINT '=========================================================================';

	END TRY

	BEGIN CATCH

		PRINT '=================================================';
		PRINT 'an error happend whene loading the bronze layer';
		PRINT '=================================================';
		PRINT 'error message: ' + ERROR_MESSAGE();
		PRINT 'error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'error state: ' + CAST(ERROR_STATE() AS NVARCHAR);

	END CATCH

END;
