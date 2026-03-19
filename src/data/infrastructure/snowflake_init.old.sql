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
    date DATE,ùù
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
  CASE
    WHEN avg_gain IS NULL OR avg_loss IS NULL THEN NULL
    WHEN avg_loss = 0 THEN 100.0
    ELSE 100.0 - 100.0 / (1.0 + (avg_gain / NULLIF(avg_loss, 0)))
  END AS rsi_14
FROM avgs
ORDER BY symbol, date;


CREATE OR REPLACE VIEW PFE_TRADING.PUBLIC.MACD_12_26_9 AS
WITH base AS (
  SELECT
    symbol,
    date,
    close,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
  FROM PFE_TRADING.PUBLIC.OHLCV
),
sma AS (
  SELECT
    symbol,
    date,
    close,
    rn,
    CASE WHEN rn >= 12 THEN AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) END AS init_ema12,
    CASE WHEN rn >= 26 THEN AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) END AS init_ema26
  FROM base
),
ema12 AS (
  SELECT symbol, rn, date, close, init_ema12 AS ema12
  FROM sma
  WHERE rn = 12
  UNION ALL
  SELECT s.symbol, s.rn, s.date, s.close,
    ( (2.0 / (12.0 + 1.0)) * s.close + (1.0 - 2.0 / (12.0 + 1.0)) * e.ema12 )
  FROM ema12 e
  JOIN sma s ON s.symbol = e.symbol AND s.rn = e.rn + 1
),
ema26 AS (
  SELECT symbol, rn, date, close, init_ema26 AS ema26
  FROM sma
  WHERE rn = 26
  UNION ALL
  SELECT s.symbol, s.rn, s.date, s.close,
    ( (2.0 / (26.0 + 1.0)) * s.close + (1.0 - 2.0 / (26.0 + 1.0)) * e.ema26 )
  FROM ema26 e
  JOIN sma s ON s.symbol = e.symbol AND s.rn = e.rn + 1
),
macd_prep AS (
  SELECT
    e12.symbol,
    e12.rn,
    e12.date,
    e12.close,
    e12.ema12,
    e26.ema26,
    (e12.ema12 - e26.ema26) AS macd
  FROM ema12 e12
  JOIN ema26 e26 ON e12.symbol = e26.symbol AND e12.rn = e26.rn
),
macd_with_rn AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS macd_rn
  FROM macd_prep
),
signal_init AS (
  SELECT symbol, macd_rn, date, macd,
    CASE WHEN macd_rn >= 9 THEN AVG(macd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) END AS init_signal
  FROM macd_with_rn
),
signal AS (
  SELECT symbol, macd_rn, date, macd, init_signal AS signal
  FROM signal_init WHERE macd_rn = 9
  UNION ALL
  SELECT sig.symbol, m.macd_rn, m.date, m.macd,
    ( (2.0 / (9.0 + 1.0)) * m.macd + (1.0 - 2.0 / (9.0 + 1.0)) * sig.signal )
  FROM signal sig
  JOIN macd_with_rn m ON m.symbol = sig.symbol AND m.macd_rn = sig.macd_rn + 1
),
final AS (
  SELECT m.symbol, m.date, m.close, m.ema12, m.ema26, m.macd, sig.signal AS macd_signal, (m.macd - sig.signal) AS macd_hist
  FROM macd_with_rn m
  JOIN signal sig ON m.symbol = sig.symbol AND m.macd_rn = sig.macd_rn
)
SELECT symbol, date, macd 
FROM final ORDER BY symbol, date;