-- DROP VIEW IF EXISTS transactions;
-- DROP TABLE IF EXISTS transactionsplus;
-- DROP TABLE IF EXISTS transactionsminus; 

ALTER TABLE transactions RENAME TO transactionsprod;

-- create transactions+
CREATE TABLE transactionsplus (
  transaction_id BIGINT    NOT NULL,
  from_acct      CHAR(12)  NOT NULL,
  to_acct        CHAR(12)  NOT NULL,
  from_route     CHAR(9)   NOT NULL,
  to_route       CHAR(9)   NOT NULL,
  amount         INT       NOT NULL,
  timestamp      TIMESTAMP NOT NULL
);

-- create transactions-
CREATE TABLE transactionsminus (
  transaction_id BIGINT    NOT NULL,
  from_acct      CHAR(12)  NOT NULL,
  to_acct        CHAR(12)  NOT NULL,
  from_route     CHAR(9)   NOT NULL,
  to_route       CHAR(9)   NOT NULL,
  amount         INT       NOT NULL,
  timestamp      TIMESTAMP NOT NULL
);

-- create transactions'
CREATE OR REPLACE VIEW transactions AS
SELECT * FROM transactionsprod
UNION ALL
SELECT * FROM transactionsplus
EXCEPT ALL
SELECT * FROM transactionsminus;

-- insert triggers
CREATE OR REPLACE FUNCTION transactions_redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_insert executed for transaction_id %', NEW.transaction_id; 
 IF EXISTS (SELECT * FROM transactions WHERE transaction_id = NEW.transaction_id) THEN
  RAISE EXCEPTION 'transaction id already exists %', NEW.transaction_id;
 ELSE
  IF NEW.transaction_id IS NULL THEN
    NEW.transaction_id := (SELECT COALESCE(MAX(transaction_id), 0) FROM transactions) + 1;
  END IF;
  INSERT INTO transactionsplus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (NEW.transaction_id, NEW.from_acct,NEW.to_acct,NEW.from_route,NEW.to_route,NEW.amount,NEW.timestamp);
  RETURN NEW;
 END IF;
END;
$$;

CREATE OR REPLACE TRIGGER transactions_redirect_insert_trigger
  INSTEAD OF INSERT ON transactions
  FOR EACH ROW
  EXECUTE PROCEDURE transactions_redirect_insert();

-- delete triggers
CREATE OR REPLACE FUNCTION transactions_redirect_delete()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger transactions_redirect_delete executed for transaction_id %', OLD.transaction_id; 
  INSERT INTO transactionsminus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (OLD.transaction_id, OLD.from_acct,OLD.to_acct,OLD.from_route,OLD.to_route,OLD.amount,OLD.timestamp);
  RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER transactions_redirect_delete_trigger
  INSTEAD OF DELETE ON transactions
  FOR EACH ROW
  EXECUTE PROCEDURE transactions_redirect_delete();

-- update triggers
CREATE OR REPLACE FUNCTION transactions_redirect_update()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger transactions_redirect_update executed for new transaction_id % old transaction_id %', NEW.transaction_id,OLD.transaction_id;
  IF EXISTS (SELECT * FROM transactions WHERE transaction_id = NEW.transaction_id) AND NEW.transaction_id != OLD.transaction_id THEN
    RAISE EXCEPTION 'transaction id already exists %', NEW.transaction_id;
  END IF;

  INSERT INTO transactionsminus (transaction_id, from_acct, to_acct,from_route,to_route,amount,timestamp) 
  VALUES (OLD.transaction_id, OLD.from_acct,OLD.to_acct,OLD.from_route,OLD.to_route,OLD.amount,OLD.timestamp);

  INSERT INTO transactionsplus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (NEW.transaction_id, NEW.from_acct,NEW.to_acct,NEW.from_route,NEW.to_route,NEW.amount,NEW.timestamp);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER transactions_redirect_update_trigger
  INSTEAD OF UPDATE ON transactions
  FOR EACH ROW
  EXECUTE PROCEDURE transactions_redirect_update();


CREATE RULE PREVENT_UPDATE AS
  ON UPDATE TO transactions
  DO INSTEAD NOTHING;

CREATE RULE PREVENT_DELETE AS
  ON DELETE TO transactions
  DO INSTEAD NOTHING;

CREATE INDEX IF NOT EXISTS transactionsminus_from_idx ON transactionsminus (from_acct, from_route, timestamp);
CREATE INDEX IF NOT EXISTS transactionsminus_to_idx ON transactionsminus (to_acct, to_route, timestamp);
CREATE INDEX IF NOT EXISTS transactionsplus_from_idx ON transactionsplus (from_acct, from_route, timestamp);
CREATE INDEX IF NOT EXISTS transactionsplus_to_idx ON transactionsplus (to_acct, to_route, timestamp);

