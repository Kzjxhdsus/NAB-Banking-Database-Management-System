CREATE OR REPLACE PROCEDURE GRIZZLY_DB.SILVER.CLEAN_TRANSACTIONS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    CREATE OR REPLACE TABLE GRIZZLY_DB.SILVER.TRANSACTIONS AS
    SELECT
        -- Primary IDs and joins
        UPPER(TRIM(UNIFIED_TXN_ID))::STRING   AS UNIFIED_TXN_ID,
        TO_DATE(TO_VARCHAR(DATE))             AS DATE,              -- safe for both DATE/VARCHAR
        UPPER(TRIM(CARD_ID))::STRING          AS CARD_ID,
        UPPER(TRIM(MERCHANT_ID))::STRING      AS MERCHANT_ID,
        CAST(CUSTOMER_ID AS STRING)           AS CUSTOMER_ID,

        -- Use_chip normalization → ''IN_STORE'' or ''ONLINE''
        CASE
            WHEN UPPER(TRIM(USE_CHIP)) IN (''Y'',''YES'',''TRUE'',''1'')  THEN ''IN_STORE''
            WHEN UPPER(TRIM(USE_CHIP)) IN (''N'',''NO'',''FALSE'',''0'')  THEN ''ONLINE''
            ELSE NULL
        END AS CHANNEL,

        -- Amount & numeric columns
        CAST(AMOUNT AS FLOAT)                 AS AMOUNT,
        NULLIF(TRIM(ERRORS), '''')              AS ERRORS,
        CAST(MCC AS STRING)                   AS MCC,

        -- Merchant info normalization
        INITCAP(TRIM(MERCHANT_CITY))          AS MERCHANT_CITY,
        UPPER(TRIM(MERCHANT_STATE))           AS MERCHANT_STATE,
        CAST(ZIP AS FLOAT)                    AS ZIP,

        -- Optional metadata
        NVL(IS_DUPLICATE, 0)                  AS IS_DUPLICATE,
        TRIM(SOURCE_SYSTEM)                   AS SOURCE_SYSTEM,
        TRIM(SOURCE_TXN_ID)                   AS SOURCE_TXN_ID,
        TRIM(MATCH_SIGNATURE)                 AS MATCH_SIGNATURE,
        TRIM(DUPE_GROUP_ID)                   AS DUPE_GROUP_ID,
        CAST(CUSTOMER_ID_STR AS STRING)       AS CUSTOMER_ID_STR

    FROM GRIZZLY_DB.BRONZE.TRANSACTIONS
    WHERE UNIFIED_TXN_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UNIFIED_TXN_ID
        ORDER BY NVL(IS_DUPLICATE,0), TO_DATE(TO_VARCHAR(DATE)) DESC
    ) = 1;

    RETURN ''CLEAN_TRANSACTIONS: Loaded to SILVER.TRANSACTIONS successfully'';
END;
';