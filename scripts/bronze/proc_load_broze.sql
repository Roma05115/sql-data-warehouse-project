
/*

procedimiento almacenado para cargar la data, limpiando los datos de la tabla en caso de que existan datos
y calculando los tiempos de carga
EXEC broze.load_broze;
*/


use DataWarehouse;
GO

CREATE OR ALTER PROCEDURE broze.load_broze AS
BEGIN
	DECLARE @tiempo_inicio AS DATETIME, 
	@tiempo_fin AS DATETIME,
	@tiempo_total_inicio as DATETIME,
	@tiempo_total_fin as DATETIME;
	BEGIN TRY

		SET @tiempo_total_inicio = GETDATE();
		PRINT '__________________________________________________________';
		PRINT 'Cargando Broze Layer ';
		PRINT '__________________________________________________________';

		PRINT '__________________________________________________________';
		PRINT 'Cargando Tablas del CRM';
		PRINT '__________________________________________________________';

		SET @tiempo_inicio = GETDATE();
		PRINT 'Truncating table: broze.crm_cust_info';
		TRUNCATE TABLE broze.crm_cust_info;
		PRINT 'Inserting table: broze.crm_cust_info';
		BULK INSERT broze.crm_cust_info
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';


		SET @tiempo_inicio = GETDATE();
		PRINT 'Truncating table: broze.crm_prd_info';
		TRUNCATE TABLE broze.crm_prd_info;
		PRINT 'Inserting table: broze.crm_prd_info';
		BULK INSERT broze.crm_prd_info
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';


		SET @tiempo_inicio = GETDATE();
		PRINT 'Truncating table: crm_sales_details';
		TRUNCATE TABLE broze.crm_sales_details;
		PRINT 'Inserting table: crm_sales_details';
		BULK INSERT broze.crm_sales_details
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';


		SET @tiempo_inicio = GETDATE();
		PRINT '__________________________________________________________';
		PRINT 'Cargando Tablas del ERP';
		PRINT '__________________________________________________________';

		PRINT 'Truncating table: broze.erp_cust_az12';
		TRUNCATE TABLE broze.erp_cust_az12;
		PRINT 'Inserting table: broze.erp_cust_az12';
		BULK INSERT broze.erp_cust_az12
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';


		SET @tiempo_inicio = GETDATE();
		PRINT 'Truncating table: broze.erp_loc_a101';
		TRUNCATE TABLE broze.erp_loc_a101;
		PRINT 'Inserting table: broze.erp_loc_a101';
		BULK INSERT broze.erp_loc_a101
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';


		SET @tiempo_inicio = GETDATE();
		PRINT 'Truncating table: broze.erp_px_cat_g1v2';
		TRUNCATE TABLE broze.erp_px_cat_g1v2;
		PRINT 'Inserting table: broze.erp_px_cat_g1v2';
		BULK INSERT broze.erp_px_cat_g1v2
		FROM 'C:\Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @tiempo_fin = GETDATE();
		PRINT 'Duración de Carga: '+ CAST (DATEDIFF(second,@tiempo_inicio, @tiempo_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';
		
		PRINT '============================================'
		SET @tiempo_total_fin = GETDATE();
		PRINT 'Duración TOTAL de Carga: '+ CAST (DATEDIFF(second,@tiempo_total_inicio, @tiempo_total_fin) AS NVARCHAR) + 'segundos';
		PRINT '------------';
	END TRY
	BEGIN CATCH
		PRINT '-------------------------------------------------'
		PRINT 'Error al cargar Broze Layer '
		PRINT 'ERROR: '+ERROR_MESSAGE();
		PRINT 'ERROR: '+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR: '+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------'
	END CATCH
END
