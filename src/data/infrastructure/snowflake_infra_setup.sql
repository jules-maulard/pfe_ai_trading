-- Create wherehouse and resource monitor
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'PFE AI Trading – main compute warehouse';


CREATE RESOURCE MONITOR IF NOT EXISTS TRADING_COST_DAILY_MONITOR
    WITH CREDIT_QUOTA = 3
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = TRADING_COST_DAILY_MONITOR;


-- -- Setup network policy
-- -- Clean any existing bindings and policies
-- ALTER USER <user> UNSET NETWORK_POLICY; -- replace with your Snowflake user
-- ALTER ACCOUNT UNSET NETWORK_POLICY;
-- DROP NETWORK POLICY IF EXISTS AUTORISE_MON_IP;

-- -- Create and add a policy allowing multiple IPs
-- CREATE NETWORK POLICY autorise_mon_ip
--   ALLOWED_IP_LIST = ('<ip1>', '<ip2>'); -- replace with your actual IPs
-- ALTER USER <user> SET NETWORK_POLICY = autorise_mon_ip; -- replace with your Snowflake user
