ALTER USER JULESMAULARD UNSET NETWORK_POLICY;
ALTER ACCOUNT UNSET NETWORK_POLICY;
DROP NETWORK POLICY IF EXISTS AUTORISE_MON_IP;


-- Créer une policy si besoin
CREATE NETWORK POLICY autorise_mon_ip
  ALLOWED_IP_LIST = ('147.161.181.117');

-- Lier la policy à votre utilisateur ou compte
ALTER USER JULESMAULARD SET NETWORK_POLICY = autorise_mon_ip;
-- ou
ALTER ACCOUNT SET NETWORK_POLICY = autorise_mon_ip;



-- D'abord supprimer l'ancienne
DROP NETWORK POLICY IF EXISTS AUTORISE_MON_IP;

-- Recréer avec les DEUX IPs (navigateur + Python)
CREATE NETWORK POLICY autorise_mon_ip
  ALLOWED_IP_LIST = ('147.161.181.102', '147.161.181.117');

-- Activer
ALTER USER JULESMAULARD SET NETWORK_POLICY = autorise_mon_ip;





-- Supprimer l'ancienne
ALTER USER JULESMAULARD UNSET NETWORK_POLICY;
DROP NETWORK POLICY IF EXISTS AUTORISE_MON_IP;

-- Recréer avec toute la plage large
CREATE NETWORK POLICY autorise_mon_ip
  ALLOWED_IP_LIST = ('147.161.0.0/16');

ALTER USER JULESMAULARD SET NETWORK_POLICY = autorise_mon_ip;







-- Créer la base de données
CREATE DATABASE IF NOT EXISTS PFE_TRADING;

-- Créer le schéma
CREATE SCHEMA IF NOT EXISTS PFE_TRADING.PUBLIC;

-- Table OHLCV
CREATE TABLE IF NOT EXISTS PFE_TRADING.PUBLIC.OHLCV (
    symbol VARCHAR,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT
);

-- Table ASSET
CREATE TABLE IF NOT EXISTS PFE_TRADING.PUBLIC.ASSET (
    symbol VARCHAR,
    company_name VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    currency VARCHAR,
    country VARCHAR,
    exchange VARCHAR,
    long_business_summary VARCHAR,
    website VARCHAR
);

-- Table DIVIDEND
CREATE TABLE IF NOT EXISTS PFE_TRADING.PUBLIC.DIVIDEND (
    symbol VARCHAR,
    date DATE,
    amount FLOAT
);








select symbol from ohlcv
group by symbol;

select * from ohlcv;

select date from ohlcv
where symbol = 'AIR.PA'
and date > '2026-02-25'
order by date;

select * from ohlcv
where date = '2026-03-06';

select max(date) as latest_update from ohlcv;

select symbol, max(date) as latest_update from ohlcv
group by symbol;

-- delete from ohlcv;

select * from ohlcv
where date = '2016-01-06';

-- delete from dividend;

select * from dividend
where date <= '2005-01-01'
order by date;

select * from asset;

select * from asset 
where company_name is null;

-- delete from asset 
-- where company_name is null;





CREATE OR REPLACE VIEW PFE_TRADING.PUBLIC.RSI_14 AS
WITH deltas AS (
  SELECT
    symbol,
    date,
    close,
    close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS delta
  FROM PFE_TRADING.PUBLIC.OHLCV
),
avgs AS (
  SELECT
    symbol,
    date,
    close,
    AVG(CASE WHEN delta > 0 THEN delta ELSE 0 END) OVER (
      PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_gain,
    AVG(CASE WHEN delta < 0 THEN -delta ELSE 0 END) OVER (
      PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_loss
  FROM deltas
)
SELECT
  symbol,
  date,
  close,
  avg_gain,
  avg_loss,
  CASE
    WHEN avg_gain IS NULL OR avg_loss IS NULL THEN NULL
    WHEN avg_loss = 0 THEN 100.0
    ELSE 100.0 - 100.0 / (1.0 + (avg_gain / NULLIF(avg_loss, 0)))
  END AS rsi_14
FROM avgs
ORDER BY symbol, date;
