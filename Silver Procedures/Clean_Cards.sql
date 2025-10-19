CREATE OR REPLACE PROCEDURE GRIZZLY_DB.SILVER.CLEAN_CARDS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    CREATE OR REPLACE TABLE GRIZZLY_DB.SILVER.CARDS AS
    SELECT
        UPPER(TRIM(CARD_ID))::STRING      AS CARD_ID,
        UPPER(TRIM(CUSTOMER_ID))::STRING  AS CUSTOMER_ID,
        INITCAP(TRIM(CARD_BRAND))         AS CARD_BRAND,
        INITCAP(TRIM(CARD_TYPE))          AS CARD_TYPE,

        -- Remove non-digit characters from card number
        REGEXP_REPLACE(CARD_NUMBER, ''[^0-9]'', '''') AS CARD_NUMBER,

        -- Robust date parsing — force string first, then test formats
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(EXPIRES)),                -- works if already date
            TRY_TO_DATE(TO_VARCHAR(EXPIRES, ''YYYY-MM-DD'')),  -- try yyyy-mm-dd
            TRY_TO_DATE(TO_VARCHAR(EXPIRES, ''YYYY-MM'')),     -- try yyyy-mm
            TRY_TO_DATE(TO_VARCHAR(EXPIRES, ''MM/YY''))        -- try mm/yy
        ) AS EXPIRES,

        CAST(CVV AS FLOAT)                AS CVV,

        CASE
            WHEN UPPER(TRIM(HAS_CHIP)) IN (''Y'',''YES'',''TRUE'',''1'') THEN TRUE
            WHEN UPPER(TRIM(HAS_CHIP)) IN (''N'',''NO'',''FALSE'',''0'') THEN FALSE
            ELSE NULL
        END AS HAS_CHIP,

        CAST(NUM_CARDS_ISSUED AS FLOAT)   AS NUM_CARDS_ISSUED,
        CAST(REPLACE(REPLACE(CREDIT_LIMIT, '','', ''''), ''$'', '''') AS FLOAT) AS CREDIT_LIMIT,

        TRY_TO_DATE(TO_VARCHAR(ACCT_OPEN_DATE)) AS ACCT_OPEN_DATE,
        CAST(YEAR_PIN_LAST_CHANGED AS FLOAT) AS YEAR_PIN_LAST_CHANGED,

        CASE
            WHEN UPPER(TRIM(CARD_ON_DARK_WEB)) IN (''Y'',''YES'',''TRUE'',''1'') THEN TRUE
            WHEN UPPER(TRIM(CARD_ON_DARK_WEB)) IN (''N'',''NO'',''FALSE'',''0'') THEN FALSE
            ELSE NULL
        END AS CARD_ON_DARK_WEB

    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY UPPER(TRIM(CARD_ID))
                   ORDER BY TRY_TO_DATE(TO_VARCHAR(ACCT_OPEN_DATE)) DESC
               ) AS RNK
        FROM GRIZZLY_DB.BRONZE.CARDS
    )
    WHERE RNK = 1
      AND CARD_ID IS NOT NULL;

    RETURN ''CLEAN_CARDS: Loaded to SILVER.CARDS successfully'';
END;
';