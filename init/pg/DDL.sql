CREATE SCHEMA if not exists financial_models;

CREATE TABLE if not exists financial_models.stocks (
	ticker_id serial4 NOT NULL,
	ticker varchar NOT NULL,
	CONSTRAINT stocks_pk PRIMARY KEY (ticker_id)
);

CREATE TABLE if not exists financial_models.stocks_daily (
	record_id serial4 NOT NULL,
	ticker_id int4 NOT NULL,
	"date" date NOT NULL,
	"open" float8 NOT NULL,
	high float8 NOT NULL,
	low float8 NOT NULL,
	"close" float8 NOT NULL,
	CONSTRAINT stocks_daily_pk PRIMARY KEY (record_id),
    CONSTRAINT stocks_daily_stocks_fk FOREIGN KEY (ticker_id) REFERENCES financial_models.stocks(ticker_id)
);

CREATE TABLE if not exists financial_models.users (
	users_id serial4 NOT NULL,
	first_name varchar NOT NULL,
	last_name varchar NOT NULL,
    email varchar NOT NULL,
    country varchar NOT NULL,
	CONSTRAINT users_pk PRIMARY KEY (users_id)
);

CREATE TABLE if not exists financial_models.trade_execution (
	trade_id serial4 NOT NULL,
	ticker_id int4 NOT NULL,
	cnt_stock numeric NOT NULL,
	trade_time timestamp NOT NULL,
	user_id int4 NOT NULL,
	trade_type varchar NOT NULL,
	CONSTRAINT trade_execution_pk PRIMARY KEY (trade_id),
    CONSTRAINT trade_execution_stocks_fk FOREIGN KEY (ticker_id) REFERENCES financial_models.stocks(ticker_id),
    CONSTRAINT trade_execution_users_fk FOREIGN KEY (user_id) REFERENCES financial_models.users(users_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_stocks_daily_ticker_date
ON financial_models.stocks_daily (ticker_id, date);


CREATE UNIQUE INDEX IF NOT EXISTS idx_uniq_users
ON financial_models.users (first_name, last_name);

CREATE UNIQUE INDEX IF NOT EXISTS idx_uniq_trade
ON financial_models.trade_execution (trade_id, cnt_stock, trade_time,user_id, trade_type);