
-- CREATE TABLE transactions (
--   transaction_id BIGINT    GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
--   from_acct      CHAR(12)  NOT NULL,
--   to_acct        CHAR(12)  NOT NULL,
--   from_route     CHAR(9)   NOT NULL,
--   to_route       CHAR(9)   NOT NULL,
--   amount         INT       NOT NULL,
--   timestamp      TIMESTAMP NOT NULL
-- );


-- create transactions+
CREATE TABLE IF NOT EXISTS transactionsplus (
  transaction_id BIGINT    GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  from_acct      CHAR(12)  NOT NULL,
  to_acct        CHAR(12)  NOT NULL,
  from_route     CHAR(9)   NOT NULL,
  to_route       CHAR(9)   NOT NULL,
  amount         INT       NOT NULL,
  timestamp      TIMESTAMP NOT NULL
);

-- create transactions-
CREATE TABLE IF NOT EXISTS transactionsminus (
  transaction_id BIGINT,
  from_acct      CHAR(12)  NOT NULL,
  to_acct        CHAR(12)  NOT NULL,
  from_route     CHAR(9)   NOT NULL,
  to_route       CHAR(9)   NOT NULL,
  amount         INT       NOT NULL,
  timestamp      TIMESTAMP NOT NULL
);

-- create transactions'
CREATE OR REPLACE VIEW transactionsprime AS
SELECT * FROM transactions
UNION ALL
SELECT * FROM transactionsplus
EXCEPT ALL
SELECT * FROM transactionsminus;

-- insert triggers
CREATE OR REPLACE FUNCTION redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_insert executed for transaction_id %', NEW.transaction_id; 
 IF EXISTS (SELECT * FROM transactionsprime WHERE id = NEW.transaction_id) THEN
  RAISE EXCEPTION 'id already exists %', OLD.transaction_id;
 ELSE
  INSERT INTO transactionsplus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (NEW.transaction_id, NEW.from_acct,NEW.to_acct,NEW.from_route,NEW.to_route,NEW.amount,NEW.timestamp);
  RETURN NEW;
 END IF;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_insert_trigger
  INSTEAD OF INSERT ON transactionsprime
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_insert();

-- delete triggers
CREATE OR REPLACE FUNCTION redirect_delete()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_delete executed for transaction_id %', OLD.transaction_id; 
  INSERT INTO transactionsminus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (OLD.transaction_id, OLD.from_acct,OLD.to_acct,OLD.from_route,OLD.to_route,OLD.amount,OLD.timestamp);
  RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_delete_trigger
  INSTEAD OF DELETE ON transactionsprime
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_delete();

-- update triggers
CREATE OR REPLACE FUNCTION redirect_update()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger redirect_update executed for new transaction_id % old transaction_id %', NEW.transaction_id,OLD.transaction_id;
  INSERT INTO transactionsminus (transaction_id, from_acct, to_acct,from_route,to_route,amount,timestamp) 
  VALUES (OLD.transaction_id, OLD.from_acct,OLD.to_acct,OLD.from_route,OLD.to_route,OLD.amount,OLD.timestamp);

  INSERT INTO transactionsplus (transaction_id, from_acct,to_acct,from_route,to_route,amount,timestamp) 
  VALUES (NEW.transaction_id, NEW.from_acct,NEW.to_acct,NEW.from_route,NEW.to_route,NEW.amount,NEW.timestamp);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_update_trigger
  INSTEAD OF UPDATE ON transactionsprime
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_update();


CREATE RULE PREVENT_UPDATE AS
  ON UPDATE TO transactionsprime
  DO INSTEAD NOTHING;

CREATE RULE PREVENT_DELETE AS
  ON DELETE TO transactionsprime
  DO INSTEAD NOTHING;

--   Indexes:
--     "transactions_pkey" PRIMARY KEY, btree (transaction_id)
--     "fromidx" btree (from_acct, from_route, "timestamp")
--     "toidx" btree (to_acct, to_route, "timestamp")
-- Rules:
--     prevent_delete AS
--     ON DELETE TO transactions DO INSTEAD NOTHING
--     prevent_update AS
--     ON UPDATE TO transactions DO INSTEAD NOTHING
