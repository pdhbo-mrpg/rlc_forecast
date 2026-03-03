--*******************************************************************************************
--*Archive existing Forecast
--*******************************************************************************************
EXECUTE IMMEDIATE $$

DECLARE
    -- Declare and initialize your variables
    LAST_ARCHIVED VARCHAR;
    LAST_FORECASTED VARCHAR;
BEGIN

    SELECT MAX(FCSTMTH) into :LAST_ARCHIVED FROM   DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH_ARCHIVE;
    SELECT  min(retail_year_month) into :LAST_FORECASTED FROM DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH;
    -- Use an IF statement to compare the variables
    IF (LAST_ARCHIVED <= LAST_FORECASTED) THEN

        INSERT INTO DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH_ARCHIVE
        SELECT
        min(retail_year_month) OVER () AS FCSTMTH ,
        m.*
        FROM DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH m;
    ELSE 
        RETURN 'Archive already exists.';

    END IF;
    RETURN 'Forecast archive completed.';

END;
$$;