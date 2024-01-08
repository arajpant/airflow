
Declare @schema_name varchar(100)='dbo'
Declare @sql_query nvarchar(max)=''
-- CHECK Schema Name exists or not.
IF NOT EXISTS (SELECT  * FROM sys.schemas  where name = @schema_name)
    BEGIN
        SET @sql_query = 'CREATE SCHEMA '+@schema_name
      --  print(@sql_query)
        EXEC(@sql_query)
    END

-- Check table exists or not 
IF NOT EXISTS (Select top 1 * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = @schema_name and TABLE_NAME = 'EntityInformations' and TABLE_TYPE='BASE TABLE')
    BEGIN
        SET @sql_query = 'Create Table ['+@schema_name+'].[EntityInformations] (
            [Id] [int] PRIMARY KEY IDENTITY(1,1) NOT NULL,
	        [Name] [nvarchar](100) NOT NULL,
	        [Status] [bit] NOT NULL
            )'
     --   print(@sql_query)
        EXEC(@sql_query)
    END

IF NOT EXISTS (Select top 1 * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = @schema_name and TABLE_NAME = 'EntityColumnInformations' and TABLE_TYPE='BASE TABLE')
    BEGIN
        SET @sql_query = 'Create Table ['+@schema_name+'].[EntityColumnInformations] (
            [Id] [bigint] PRIMARY KEY IDENTITY(1,1) NOT NULL,
	        [EntityId] [int] NOT NULL ,
	        [Name] [nvarchar](100) NOT NULL,
	        [DataType] [nvarchar](100) NULL,
	        [DataLength] [nvarchar](100) NULL,
	        [Status] [bit] NOT NULL,
            CONSTRAINT FK_'+@schema_name+'_EntityColumnInformations_EntityInformations FOREIGN KEY (EntityId)
            REFERENCES ['+@schema_name+'].[EntityInformations](Id)
            )
            '
       -- print(@sql_query)
        EXEC(@sql_query)
    END
IF NOT EXISTS(SELECT * FROM sys.objects WHERE type = 'P' AND OBJECT_ID = OBJECT_ID(@schema_name+'.sp_transformationRule'))
    BEGIN
        SET @sql_query = '
        CREATE OR ALTER PROC ['+@schema_name+'].[sp_transformationRules] 
        @SchemaName NVARCHAR(100),
        @MainTableName NVARCHAR(300),
        @DateTableName NVARCHAR(400),
        @PrimaryKey NVARCHAR(100)=''Id''

        /*   
        -- =============================================     
        -- Author:    Ananta Raj Pant
        -- Company Name: Devfinity LLC
        --- Designation: Principal Data Engineer.
        -- Description: Check the Table and Update the properties.  
        -- =============================================
        */   

        AS BEGIN 
            SET NOCOUNT ON    
            -- Check table exists or not. 
            -- check for primary key count. 
            DECLARE @primary_key_table TABLE (PrimaryKeyColumnName NVARCHAR(100)) 
            Insert into @primary_key_table 
            SELECT * FROM STRING_SPLIT(@PrimaryKey, '',''); 
            DECLARE @query nvarchar(max)='''' 
            DECLARE @outputColumnTable TABLE (Columnname NVARCHAR(300)) 
            DECLARE @columnValue NVARCHAR(max)='''' 
             
                BEGIN 
                    IF EXISTS (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @MainTableName AND TABLE_SCHEMA = @SchemaName) 
                        BEGIN 
                            PRINT(''TABLE EXISTS'') 
                            -- Get column name in @outputColumnTable 
                            DECLARE @joining_primary_key VARCHAR(MAX)='''' 
                            DECLARE @PrimaryKey_Name_From_Table VARCHAR(100)=''''
                            INSERT INTO @outputColumnTable 
                            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE Table_Schema =@SchemaName AND TABLE_NAME=@MainTableName ORDER BY ORDINAL_POSITION ASC 
                            -- If exists then check duplication in main table.  
                             
                            SELECT  @columnValue = STRING_AGG(Columnname, '','') FROM @outputColumnTable 
                            
                            SET @query  = '' BEGIN TRY                                     
                                                BEGIN TRANSACTION   
                                                    ;WITH CTE AS ( SELECT Date_table.* FROM ( 
                                                        SELECT * ,ROW_NUMBER() OVER(PARTITION BY ''+@PrimaryKey+'' ORDER BY ''+@PrimaryKey+'' ASC) AS RN_Value 
                                                            FROM 
                                                        [''+@SchemaName+''].[''+@DateTableName+''] 
                                                            ) Date_table 
                                                            WHERE Date_table.RN_Value > 1 ) 
                                                            DELETE FROM CTE ; 
 
                                                    DELETE mainTable FROM [''+@SchemaName+''].[''+@MainTableName+''] mainTable  
                                                    INNER JOIN [''+@SchemaName+''].[''+@dateTableName+''] dateTable  
                                                    '' 
                                                    WHILE EXISTS (SELECT TOP 1 PrimaryKeyColumnName FROM @primary_key_table ORDER BY PrimaryKeyColumnName ASC) 
                                                        BEGIN 
                                                            SELECT TOP 1 @PrimaryKey_Name_From_Table = PrimaryKeyColumnName FROM @primary_key_table 
                                                            ORDER BY PrimaryKeyColumnName ASC 
                                                            IF(@joining_primary_key='''') 
                                                                BEGIN 
                                                                    SET @joining_primary_key = ''ON''
                                                                END 
                                                            ELSE  
                                                                BEGIN 
                                                                    SET @joining_primary_key = @joining_primary_key + '' AND '' 
                                                                END 
                                                            SET @joining_primary_key = @joining_primary_key + '' mainTable.[''+@PrimaryKey_Name_From_Table+''] = dateTable.[''+@PrimaryKey_Name_From_Table+'']'' 
                                                            DELETE FROM @primary_key_table WHERE PrimaryKeyColumnName = @PrimaryKey_Name_From_Table 
                                                        END 
                            SET @query = @query + @joining_primary_key + '' INSERT INTO [''+@SchemaName+''].[''+@MainTableName+''] 
                                                    SELECT '' +@columnValue+ '' FROM   [''+@SchemaName+''].[''+@dateTableName+''] 
                                                COMMIT TRANSACTION   
                                                DROP TABLE [''+@SchemaName+''].[''+@dateTableName+'']
                                                SELECT 1 AS [STATUS]  

                                            END TRY                                                  
                                            BEGIN CATCH                                                  
                                                ROLLBACK TRANSACTION           
                                                SELECT 0 AS [STATUS]                                                                 
                                            END CATCH   
                                            '' 
                            PRINT(@query) 
                            EXEC(@query) 
                        END 
                    ELSE 
                        BEGIN 
                            -- if not then create MainTable and sync all the data from the date table. 
                            PRINT(''TABLE DOES NOT EXIST'') 
                            SET @query = ''
                                        BEGIN TRY                                     
                                                BEGIN TRANSACTION   
                                                    ;WITH CTE AS ( SELECT Date_table.* FROM ( 
                                                        SELECT * ,ROW_NUMBER() OVER(PARTITION BY ''+@PrimaryKey+'' ORDER BY ''+@PrimaryKey+'' ASC) AS RN_Value 
                                                            FROM 
                                                        [''+@SchemaName+''].[''+@DateTableName+''] 
                                                            ) Date_table 
                                                            WHERE Date_table.RN_Value > 1 ) 
                                                            DELETE FROM CTE ; 
 
                                                    SELECT * INTO [''+@SchemaName+''].[''+@MainTableName+''] FROM [''+@SchemaName+''].[''+@dateTableName+''] 
                                                COMMIT TRANSACTION   
                                                DROP TABLE [''+@SchemaName+''].[''+@dateTableName+'']
                                                SELECT 1 AS [STATUS]  
                                            END TRY                                                  
                                            BEGIN CATCH                                                  
                                                ROLLBACK TRANSACTION         
                                                SELECT 0 AS [STATUS]                                                                   
                                            END CATCH   
                                        '' 
                            PRINT(@query) 
                            EXEC(@query) 
                        END 
                END 
         
         
        END   '
        -- print(@sql_query)
        exec(@sql_query)
    END


IF NOT EXISTS(SELECT * FROM sys.objects WHERE type= 'P' AND OBJECT_ID = OBJECT_ID(@schema_name+'.sp_entity_information_update'))
    BEGIN
        SET @sql_query = ' 
        CREATE OR ALTER PROC ['+@schema_name+'].[sp_entity_information_update] 
        /*    
        -- =============================================      
        -- Author:    Ananta Raj Pant 
        -- Create Date: 2023/02/13    
        -- Description: Insert or Update to the table Entity Informations and Entity Column Informations.      
        -- Branch Name: story:     
        -- =============================================      
        -- Test: exec ['+@schema_name+'].sp_entity_information_update ''crm'',''account'',''account_column_attributes_20230213'' 
        */    
        @schema_name nvarchar(100)='''', 
        @EntityName nvarchar(100)='''', 
        @ColumnAttributeTableName nvarchar(400)  ='''', 
        @PrimaryKey nvarchar(400)=''''

        AS BEGIN  
            SET NOCOUNT ON 
            DECLARE @primary_key_table TABLE (PrimaryKeyColumnName NVARCHAR(100)) 
            Insert into @primary_key_table 
            SELECT * FROM STRING_SPLIT(@PrimaryKey, '',''); 
             DECLARE @EntityInfoId VARCHAR(100)='''' 
            DECLARE @Status BIT  
            DECLARE @Message NVARCHAR(MAX)  
            IF EXISTS (SELECT top 1 * from [''+@schema_name+''].[EntityInformations] where Name = @EntityName)
            BEGIN
                Select top 1 @EntityInfoId = cast([Id] as nvarchar(100)), @Status = [Status] 
                from [''+@schema_name+''].[EntityInformations] 
                where Name = @EntityName
            END
            ELSE
                BEGIN
                    BEGIN TRY              
                        BEGIN TRANSACTION  
                            declare @IdentityOutput table ( Id int )
                            DECLARE @IdentityValue int =0
                            Insert into [''+@schema_name+''].[EntityInformations]
                                OUTPUT inserted.Id into @IdentityOutput
                            VALUES(@EntityName, 1)
                            
                        COMMIT TRANSACTION       
                            select @IdentityValue = (select Id from @IdentityOutput)                                  
                            Select @EntityInfoId = cast(@IdentityValue as VARCHAR(100))  , @Status = 1    

                        END TRY                                                 
                        BEGIN CATCH                                                 
                            ROLLBACK TRANSACTION 
                                SET @EntityInfoId = ''0''
                                SET @Status = 0                                
                        END CATCH
                END
            IF (@EntityInfoId !=''0'' AND @Status !=0)
                BEGIN
                    IF EXISTS (SELECT top 1 * from [''+@schema_name+''].[EntityColumnInformations] where EntityId = @EntityInfoId)
                        BEGIN
                            DECLARE @entityColumnInsertQuery NVARCHAR(MAX)=''''
                            BEGIN TRY              
                                BEGIN TRANSACTION  
                                Delete from [''+@schema_name+''].EntityColumnInformations
                                        WHERE EntityId =@EntityInfoId;
                                COMMIT TRANSACTION
                            END TRY                                                 
                            BEGIN CATCH                                                 
                                ROLLBACK TRANSACTION 
                            END CATCH
                                SET @entityColumnInsertQuery =''
                                BEGIN TRY              
                                    BEGIN TRANSACTION  
                                        Insert into [''+@schema_name+''].[EntityColumnInformations]
                                            SELECT @EntityInfoId as [EntityId], dt.Column_Name as [Name],
                                                dt.Data_Type as [DataType],
                                                dt.Data_Type_Length as [DataLength] , 1 as [Status],
                                                (SELECT CASE WHEN EXISTS(SELECT 1 FROM STRING_SPLIT(''''+@PrimaryKey+'''', '''','''') 
                                                WHERE [value] = dt.Column_Name) THEN 1 ELSE 0 END ) AS [isPrimaryKey]
                                                from [''+@schema_name+''].[''+@ColumnAttributeTableName+''] dt
                                    COMMIT TRANSACTION      
                                    DROP TABLE [''+@schema_name+''].[''+@ColumnAttributeTableName+'']  
                                    SELECT CAST(1 AS BIT) AS [STATUS]
                                END TRY 
                                BEGIN CATCH                                                 
                                    ROLLBACK TRANSACTION 
                                    SELECT CAST(0 AS BIT) AS [STATUS]
                                END CATCH
                                ''
                                EXEC(@entityColumnInsertQuery)
                        END
                    ELSE
                        BEGIN
                            print(''Not Exists'')
                        
                                    DECLARE @query nvarchar(MAX)=''''
                                    SET @query =''
                                    BEGIN TRY              
                                        BEGIN TRANSACTION  
                                            Insert into [''+@schema_name+''].EntityColumnInformations
                                                SELECT ei.Id as [EntityId], dt.Column_Name as [Name],
                                                    dt.Data_Type as [DataType],
                                                    dt.Data_Type_Length as [DataLength] , 1 as [Status]
                                                    from [''+@schema_name+''].[''+@ColumnAttributeTableName+''] dt
                                                LEFT JOIN [''+@schema_name+''].[EntityInformations] ei
                                                    on ei.Name = dt.entity_name
                                                WHERE ei.Id is not null
                                        COMMIT TRANSACTION
                                            SELECT CAST(1 AS BIT) AS [STATUS]
                                    END TRY                                                 
                                    BEGIN CATCH                                                 
                                        ROLLBACK TRANSACTION 
                                            SELECT CAST(0 AS BIT) AS [Status]                            
                                    END CATCH
                                        ''
                                    print(@query)
                                    EXEC(@query)
                        END
                END
            ELSE
                BEGIN
                    print(''Failed to insert records in Entity Information.'')
                END
        END 
        '
       -- PRINT(@sql_query)
        EXEC(@sql_query)
    END

