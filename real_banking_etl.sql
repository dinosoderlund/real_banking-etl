USE ETL_Assignment1;
GO

/*
This solution is built on top of provided setup script Assignment_1_Real-World_Banking_ETL.sql
that creates the database and reference tables
run that before executing this etl script 
i had to manually change owner to sa when i ran sql server agent jobs (Banking ETL - Monthly) and then it ran without problems 
i did that by going on properties and changing owner to sa 
(i also used azure data studio for formatting all of the code)
*/


--Creating the ETL Log table that stores what happend each run such as errors, warnings and processed rows
IF OBJECT_ID('dbo.ETL_log', 'U') IS NULL
BEGIN

    CREATE TABLE dbo.ETL_log
    (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        --unique id for each log entry
        RUN_ID UNIQUEIDENTIFIER NOT NULL,
        -- unique identifier for each etl run
        LogLevel NVARCHAR(10) NOT NULL,
        --info / warn / error
        LogMessage NVARCHAR(4000) NOT NULL,
        --description of log run
        RowsProcessed INT NULL,
        --number of rows inserted into target table
        OutputTable SYSNAME NULL,
        --name of output table
        CreatedAtUTC DATETIME2(0) 
        --when the log was created
    )
END
GO



--Config table: contains email settings used for sending notification about success/failure 
IF OBJECT_ID('dbo.ETL_config', 'U') IS NULL
BEGIN

    CREATE TABLE dbo.ETL_config
    (
        ConfigKEY SYSNAME PRIMARY KEY,
        --MailProfile, MailRecipients
        ConfigVal NVARCHAR(4000) NOT NULL
        -- their corresponding values
    )
    --sending notification to my school mail 
    INSERT INTO dbo.ETL_config
        (ConfigKEY, ConfigVal)
    VALUES('MailProfile', 'BankETLProfile'),
        ('MailRecipients', 'dino.soderlund@yh.nackademin.se')


END
GO


--Stored procedures
--Executes pipeline for the current or given month 
IF OBJECT_ID('dbo.usp_RunBankingETL', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_RunBankingETL
GO
CREATE PROCEDURE dbo.usp_RunBankingETL
    @RunDate DATE = NULL
AS
BEGIN

    SET NOCOUNT ON

    --Declare variables used across the etl pipeline process
    DECLARE 
@RunID UNIQUEIDENTIFIER = NEWID(), --used to identify etl run
@Today DATE = CAST(GETDATE() AS DATE),
@EffDate DATE,
@YYYYMM CHAR(6),
@SRC SYSNAME, --source table
@OutTable SYSNAME, --target table
@BaseRate DECIMAL(5,2),
@BaseMonthRate DATE,
@UsedFallback BIT = 0,
@Rows INT = 0,
@msg NVARCHAR(4000),
@Mailprofile NVARCHAR(200),
@MailRecipients NVARCHAR(1000),
@SubjectFail NVARCHAR(200) = NULL,   
@SubjectOk NVARCHAR(200) = NULL,   
@BodyOk NVARCHAR(MAX) = NULL;

    --determine which month to process if rundate is not provided use the current month
    SET @EffDate = DATEFROMPARTS(YEAR(COALESCE(@RunDate, @Today)), MONTH(COALESCE(@RunDate, @Today)), 1)
    SET @YYYYMM = CONVERT(CHAR(6), @EffDate, 112)
    --building table names based on month
    SET @SRC = CONCAT('dbo.CarInformation_', @YYYYMM)
    SET @OutTable = CONCAT('dbo.LoanProfitEstimates_', @YYYYMM)

    --load email config from etl_config table
    SELECT
        @Mailprofile = MAX(CASE WHEN ConfigKEY='MailProfile' THEN ConfigVal ELSE N'' END),
        @MailRecipients = MAX(CASE WHEN ConfigKEY='MailRecipients' THEN ConfigVal ELSE N'' END)
    FROM dbo.ETL_config

    BEGIN TRY 
--validate source table
--if carinformation_yyyymm does not eixst log error and stop
IF OBJECT_ID(@SRC, 'U') IS NULL
BEGIN

        --LOG ERROR MESSAGE
        SET @msg = CONCAT('CarInformation_', @YYYYMM, ' table for current month not found!')
        INSERT INTO dbo.ETL_log
            (RUN_ID, LogLevel, LogMessage)
        VALUES(@RunID, 'ERROR', @msg)

        --fail-mail
        IF @Mailprofile IS NOT NULL AND @MailRecipients IS NOT NULL
BEGIN
            SET @SubjectFail = N'[ETL FAIL] ' + CAST(@OutTable as nvarchar(128))

            EXEC msdb.dbo.sp_send_dbmail
@profile_Name = @Mailprofile,
@recipients = @MailRecipients,
@subject = @SubjectFail,
@body = @msg
        END

        RETURN
    END

--get base interest rate, get the most recent available rate up to current month
;WITH
        row
        AS
        
        (
            SELECT TOP(1)
                BaseInterestRate, DATEFROMPARTS(YEAR(EffectiveDate), MONTH(EffectiveDate),1) AS RateMonth
            FROM InterestRates
            WHERE EffectiveDate <= @EffDate
            ORDER BY EffectiveDate DESC
        )
    SELECT @BaseRate = BaseInterestRate, @BaseMonthRate = RateMonth
    FROM row

--abort if no rate  fallback if needed
IF @BaseRate IS NULL
BEGIN
        SET @msg = 'No base interest rate available up to current month. Aborting'
        INSERT INTO dbo.ETL_log
            (RUN_ID, LogLevel, LogMessage)
        VALUES(@RunID, 'ERROR', @msg)

        IF @Mailprofile IS NOT NULL AND @MailRecipients IS NOT NULL 
BEGIN
            SET @SubjectFail = N'[ETL FAIL] ' + CAST(@OutTable AS NVARCHAR(128))
            EXEC msdb.dbo.sp_send_dbmail
@profile_name = @Mailprofile,
@recipients = @MailRecipients,
@subject = @SubjectFail,
@body = @msg
        END

        RETURN
    END

IF @BaseMonthRate <> @EffDate
BEGIN
        SET @UsedFallback = 1
        SET @msg = 'Base interest for current month missing, value from last month was used'
        INSERT INTO dbo.ETL_log
            (RUN_ID, LogLevel, LogMessage)
        VALUES(@RunID, 'WARN', @msg)
    END 

--Result table
--drop if exist, then create new table
DECLARE @sql NVARCHAR(MAX) = N'
IF OBJECT_ID(''' + @OutTable + N''', ''U'') IS NOT NULL
DROP TABLE ' + QUOTENAME(PARSENAME(@OutTable,2)) + N'.' + QUOTENAME(PARSENAME(@OutTable,1)) + N';
CREATE TABLE ' + QUOTENAME(PARSENAME(@OutTable,2)) + N'.' + QUOTENAME(PARSENAME(@OutTable,1)) + N'(
RecordID INT NOT NULL,
CarModel VARCHAR(100),
EnergyClass VARCHAR(20),
ManufactureYear INT,
BasePrice DECIMAL(10,2),
CustomerRiskTier VARCHAR(20),
FinalInterestRate DECIMAL(10,4),
EstimatedMonthlyPayment DECIMAL(18,4),
DepreciatedValue DECIMAL(18,4),
EstimatedProfit DECIMAL(18,4),
FileMonth DATE NOT NULL
)'
EXEC sys.sp_executesql @sql

--load transformed data into output table 
--calculate finalinterestrate, monthlypayment, depreciatedvalue and profit
SET @sql = N'
INSERT INTO ' + QUOTENAME(PARSENAME(@OutTABLE,2)) + N'.' + QUOTENAME(PARSENAME(@OutTable,1)) + N'(
RecordID, CarModel, EnergyClass, ManufactureYear, BasePrice, CustomerRiskTier, FinalInterestRate, EstimatedMonthlyPayment, DepreciatedValue, EstimatedProfit, FileMonth
)
SELECT
c.RecordID,
c.CarModel,
c.EnergyClass,
c.ManufactureYear,
c.BasePrice,
c.CustomerRiskTier,

CAST((' + CAST(@BaseRate AS NVARCHAR(20)) + N' + ecm.MarginRate + crt.RiskAdjustment) AS DECIMAL(10,4)) AS FinalInterestRate,
CAST((c.BasePrice * (' + CAST(@BaseRate AS NVARCHAR(20)) + N' + ecm.MarginRate  + crt.RiskAdjustment) / 100.0) / 12.0 AS DECIMAL(18,4)) AS EstimatedMonthlyPayment,
CAST((c.BasePrice * (1 - dr.DepreciationRate)) AS DECIMAL(18,4)) AS DepreciatedValue,
CAST(((c.BasePrice * (' + CAST(@BaseRate AS NVARCHAR(20)) + N' + ecm.MarginRate + crt.RiskAdjustment) / 100.0))
- (c.BasePrice - (c.BasePrice * (1 - dr.DepreciationRate))) AS DECIMAL(18,4)) AS EstimatedProfit, c.FileMonth
FROM ' + QUOTENAME(PARSENAME(@SRC,2)) + N'.' + QUOTENAME(PARSENAME(@SRC,1)) + N' AS c
JOIN dbo.EnergyClassMargin ecm
ON c.EnergyClass = ecm.EnergyClass
JOIN dbo.CreditRiskTier crt
ON c.CustomerRiskTier = crt.RiskTier
JOIN dbo.DepreciationRates dr 
ON (YEAR(''' + CONVERT(CHAR(10), @EffDate, 120) + N''') - c.ManufactureYear) BETWEEN dr.MinYear AND dr.MaxYear
';
 EXEC sys.sp_executesql @sql

 --log success and send to mail
 SET @Rows = @@ROWCOUNT
 INSERT INTO dbo.ETL_log
        (RUN_ID, LogLevel, LogMessage, RowsProcessed, OutputTable)
    VALUES(@RunID, 'INFO', 'Output table created successfully!', @Rows, @OutTable)

 IF @mailProfile IS NOT NULL AND @mailRecipients IS NOT NULL
 BEGIN
        SET @SubjectOk = N'[ETL OK] ' + CAST(@OutTable AS NVARCHAR(128));
        SET @BodyOk    = N'Rows processed: ' + CAST(@Rows AS VARCHAR(20))
+ CHAR(13) + CHAR(10) + N'Output Table: ' + CAST(@OutTable AS NVARCHAR(128))
+ CASE WHEN @UsedFallback = 1 
THEN CHAR(13) + CHAR(10) + N'Base rate fallback was used.'
ELSE N'' END;

        EXEC msdb.dbo.sp_send_dbmail
 @profile_name = @Mailprofile,
 @recipients = @MailRecipients,
 @subject = @SubjectOk,
 @body = @BodyOk
    END
 END TRY

 --error handling, logs the error message and sends to mail 
 BEGIN CATCH 
 SET @msg = CONCAT('Error: ', ERROR_MESSAGE())
 INSERT INTO dbo.ETL_log
        (RUN_ID, LogLevel, LogMessage)
    VALUES(@RunID, 'ERROR', @msg)

 IF @Mailprofile IS NOT NULL AND @MailRecipients IS NOT NULL
 BEGIN
        SET @SubjectFail = N'[ETL FAIL] ' + CAST(@OutTable AS NVARCHAR(128))

        EXEC msdb.dbo.sp_send_dbmail
 @profile_name = @Mailprofile,
 @recipients = @MailRecipients,
 @subject = @SubjectFail,
 @body = @msg

    END;
 THROW; --rethrow for sql agent to get 
 END CATCH
END
 GO

--SQL AGENT 
--automatically schedule the etl procedure to run on 1st of every month at 07:00
IF EXISTS(SELECT 1
FROM msdb.dbo.sysjobs
WHERE name = N'Banking ETL - Monthly')
 EXEC msdb.dbo.sp_delete_job @job_name = N'Banking ETL - Monthly'

--Create new sql agent job 
EXEC msdb.dbo.sp_add_job
 @job_name = N'Banking ETL - Monthly',
 @enabled = 1,
 @description = N'Creating LoanProfitEstimates__yyyymm from carinformation_yyyymm'

--adding job step(run procedure)
EXEC msdb.dbo.sp_add_jobstep
 @job_name = N'Banking ETL - Monthly',
 @step_name = N'Run dbo.usp_RunBankingETL',
 @subsystem = N'TSQL',
 @command = N'EXEC ETL_Assignment1.dbo.usp_RunBankingETL;',
 @on_fail_action = 2

--creating sechedule(1st of every month at 07:00
EXEC msdb.dbo.sp_add_schedule
 @schedule_name = N'Monthly_1st_07',
 @freq_type = 16,
 @freq_interval = 1,
 @freq_recurrence_factor = 1,
 @active_start_time = 70000

--link schedule to job 
EXEC msdb.dbo.sp_attach_schedule
 @job_name = N'Banking ETL - Monthly',
 @schedule_name = N'Monthly_1st_07'

--assign job to sql server agent 
EXEC msdb.dbo.sp_add_jobserver
 @job_name = N'Banking ETL - Monthly'
 GO



-- for business analyst 
SELECT * FROM dbo.LoanProfitEstimates_202505;

