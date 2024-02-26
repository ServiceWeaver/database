-- DROP VIEW IF EXISTS users;
-- DROP TABLE IF EXISTS usersplus;
-- DROP TABLE IF EXISTS usersminus;
-- DROP VIEW IF EXISTS contacts;
-- DROP TABLE IF EXISTS contactsplus;
-- DROP TABLE IF EXISTS contactsminus;

ALTER TABLE users RENAME TO usersprod;

-- create users+
CREATE TABLE usersplus (
     accountid CHAR(12)    NOT NULL,
     username  VARCHAR(64) NOT NULL,
     passhash  BYTEA       NOT NULL,
     firstname VARCHAR(64) NOT NULL,
     lastname  VARCHAR(64) NOT NULL,
     birthday  DATE        NOT NULL,
     timezone  VARCHAR(8)  NOT NULL,
     address   VARCHAR(64) NOT NULL,
     state     CHAR(2)     NOT NULL,
     zip       VARCHAR(5)  NOT NULL,
     ssn       CHAR(11)    NOT NULL
);

-- create users-
CREATE TABLE  usersminus (
    accountid CHAR(12)    NOT NULL,
    username  VARCHAR(64) NOT NULL,
    passhash  BYTEA       NOT NULL,
    firstname VARCHAR(64) NOT NULL,
    lastname  VARCHAR(64) NOT NULL,
    birthday  DATE        NOT NULL,
    timezone  VARCHAR(8)  NOT NULL,
    address   VARCHAR(64) NOT NULL,
    state     CHAR(2)     NOT NULL,
    zip       VARCHAR(5)  NOT NULL,
    ssn       CHAR(11)    NOT NULL
);

-- create users'
CREATE VIEW users AS
SELECT * FROM usersprod
UNION ALL
SELECT * FROM usersplus
EXCEPT ALL
SELECT * FROM usersminus;

-- insert triggers
CREATE OR REPLACE FUNCTION users_redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger users_redirect_insert executed for accountid %', NEW.accountid; 
 IF EXISTS (SELECT * FROM users WHERE accountid = NEW.accountid) THEN
  RAISE EXCEPTION 'account id already exists %', NEW.accountid;
  RETURN NULL;
 END IF;
 IF EXISTS (SELECT * FROM users WHERE username = NEW.username) THEN
  RAISE EXCEPTION 'username already exists %', NEW.username;
  RETURN NULL;
 END IF;
INSERT INTO usersplus (accountid, username,passhash,firstname,lastname,birthday,timezone,address,state,zip,ssn) 
VALUES (NEW.accountid, NEW.username, NEW.passhash, NEW.firstname, NEW.lastname, NEW.birthday,NEW.timezone,NEW.address,NEW.state,NEW.zip,NEW.ssn);
RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER users_redirect_insert_trigger
  INSTEAD OF INSERT ON users
  FOR EACH ROW
  EXECUTE PROCEDURE users_redirect_insert();


-- delete triggers
CREATE OR REPLACE FUNCTION users_redirect_delete()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger users_redirect_delete executed for accountid %', OLD.accountid; 
 IF EXISTS (SELECT * FROM contacts WHERE username = OLD.username) THEN
  RAISE EXCEPTION 'violates foreign key constraint';
 END IF;
 INSERT INTO usersminus (accountid, username,passhash,firstname,lastname,birthday,timezone,address,state,zip,ssn) VALUES (OLD.accountid, OLD.username, OLD.passhash, OLD.firstname, OLD.lastname, OLD.birthday,OLD.timezone, OLD.address, OLD.state, OLD.zip, OLD.ssn); 
 RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER users_redirect_delete_trigger
  INSTEAD OF DELETE ON users
  FOR EACH ROW
  EXECUTE PROCEDURE users_redirect_delete();

-- update triggers
CREATE OR REPLACE FUNCTION users_redirect_update()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger users_redirect_update executed for new accountid % old accountid % new username % old username %', NEW.accountid,OLD.accountid,NEW.username, OLD.username;
  IF EXISTS (SELECT * FROM contacts WHERE username = OLD.username) AND NEW.username != old.username THEN
    RAISE EXCEPTION 'violates foreign key constraint';
  END IF;
  INSERT INTO usersminus (accountid, username,passhash,firstname,lastname,birthday,timezone,address,state,zip,ssn)  
  VALUES (OLD.accountid, OLD.username, OLD.passhash, OLD.firstname, OLD.lastname, OLD.birthday,OLD.timezone, OLD.address, OLD.state, OLD.zip, OLD.ssn);

  INSERT INTO usersplus (accountid, username,passhash,firstname,lastname,birthday,timezone,address,state,zip,ssn) 
  VALUES (NEW.accountid, NEW.username, NEW.passhash, NEW.firstname, NEW.lastname, NEW.birthday,NEW.timezone,NEW.address,NEW.state,NEW.zip,NEW.ssn);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER users_redirect_update_trigger
  INSTEAD OF UPDATE ON users
  FOR EACH ROW
  EXECUTE PROCEDURE users_redirect_update();

CREATE INDEX IF NOT EXISTS usersplus_accountid_idx ON usersplus (accountid);
CREATE INDEX IF NOT EXISTS usersplus_username_idx ON usersplus (username);
CREATE INDEX IF NOT EXISTS usersminus_accountid_idx ON usersplus (accountid);
CREATE INDEX IF NOT EXISTS usersminus_username_idx ON usersplus (username);


ALTER TABLE contacts RENAME TO contactsprod;

-- create contacts+
CREATE TABLE contactsplus (
  username    VARCHAR(64)  NOT NULL,
  label       VARCHAR(128) NOT NULL,
  account_num CHAR(12)     NOT NULL,
  routing_num CHAR(9)      NOT NULL,
  is_external BOOLEAN      NOT NULL
);

-- create contacts-
CREATE TABLE contactsminus (
  username    VARCHAR(64)  NOT NULL,
  label       VARCHAR(128) NOT NULL,
  account_num CHAR(12)     NOT NULL,
  routing_num CHAR(9)      NOT NULL,
  is_external BOOLEAN      NOT NULL
);

-- create contacts'
CREATE OR REPLACE VIEW contacts AS
SELECT * FROM contactsprod
UNION ALL
SELECT * FROM contactsplus
EXCEPT ALL
SELECT * FROM contactsminus;


-- insert triggers
CREATE OR REPLACE FUNCTION contacts_redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger contacts_redirect_insert executed for username %', NEW.username;
 IF NOT EXISTS (SELECT * FROM users WHERE username = NEW.username) THEN
  RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in users table';
 END IF;
 INSERT INTO contactsplus (username,label,account_num,routing_num,is_external) 
 VALUES (NEW.username, NEW.label, NEW.account_num, NEW.routing_num, NEW.is_external);
 RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER contacts_redirect_insert_trigger
  INSTEAD OF INSERT ON contacts
  FOR EACH ROW
  EXECUTE PROCEDURE contacts_redirect_insert();

-- delete triggers
CREATE OR REPLACE FUNCTION contacts_redirect_delete()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger contacts_redirect_delete executed for username %', OLD.username; 
  INSERT INTO contactsminus (username,label,account_num,routing_num,is_external)  
  VALUES (OLD.username, OLD.label,OLD.account_num,OLD.routing_num,OLD.is_external);
  RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER contacts_redirect_delete_trigger
  INSTEAD OF DELETE ON contacts
  FOR EACH ROW
  EXECUTE PROCEDURE contacts_redirect_delete();


-- update triggers
CREATE OR REPLACE FUNCTION contacts_redirect_update()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger contacts_redirect_update executed for new username % old username %', NEW.username,OLD.username;
  IF NOT EXISTS (SELECT * FROM users WHERE username = NEW.username) THEN
   RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in users table';
  END IF;
  INSERT INTO contactsminus (username,label,account_num,routing_num,is_external) VALUES (OLD.username, OLD.label,OLD.account_num,OLD.routing_num,OLD.is_external);
  INSERT INTO contactsplus (username,label,account_num,routing_num,is_external) VALUES (NEW.username, NEW.label, NEW.account_num, NEW.routing_num, NEW.is_external);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER contacts_redirect_update_trigger
  INSTEAD OF UPDATE ON contacts
  FOR EACH ROW
  EXECUTE PROCEDURE contacts_redirect_update();

