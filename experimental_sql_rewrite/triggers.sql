DROP VIEW IF EXISTS usersprime;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS usersplus;
DROP TABLE IF EXISTS usersminus;

CREATE TABLE IF NOT EXISTS users (
    id        INT PRIMARY KEY,
    name varchar(80)
);

CREATE TABLE IF NOT EXISTS usersplus (
    id        INT,
    name varchar(80)
);

CREATE TABLE IF NOT EXISTS usersminus (
    id        INT,
    name varchar(80)
);

INSERT INTO users VALUES (1,'user1');
INSERT INTO users VALUES (2,'user2');
INSERT INTO users VALUES (3,'user3');
INSERT INTO users VALUES (4,'user4');
INSERT INTO users VALUES (5,'user5');

-- CREATE R' as view
CREATE OR REPLACE VIEW USERSPRIME AS
		SELECT * FROM users
		UNION ALL
		SELECT * FROM usersplus
    EXCEPT ALL
    SELECT * FROM usersminus;

-- INSERT 
CREATE OR REPLACE FUNCTION redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_insert executed for ID % NAME %', NEW.id, NEW.name; 
 IF EXISTS (SELECT * FROM USERSPRIME WHERE id = NEW.id) THEN
  RAISE EXCEPTION 'id already exists %', OLD.id;
 ELSE
  INSERT INTO usersplus (name, id) 
  VALUES (NEW.name, NEW.id);
  RETURN NEW;
 END IF;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_insert_trigger
  INSTEAD OF INSERT ON USERSPRIME
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_insert();

-- DELETE
CREATE OR REPLACE FUNCTION redirect_delete()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_delete executed for ID % NAME %', OLD.id, OLD.name; 
  INSERT INTO usersminus (id,name) 
  VALUES (OLD.id, OLD.name);
  RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_delete_trigger
  INSTEAD OF DELETE ON usersprime
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_delete();

-- UPDATE
CREATE OR REPLACE FUNCTION redirect_update()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  RAISE NOTICE 'Trigger redirect_update executed for NEW ID % New NAME % OLD ID % OLD name %', NEW.id,NEW.name,OLD.id,OLD.name;
  INSERT INTO usersminus (id,name) VALUES (OLD.id,OLD.name);
  INSERT INTO usersplus (id,name) VALUES (NEW.id,NEW.name);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_update_trigger
  INSTEAD OF UPDATE ON USERSPRIME
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_update();

-- examples
INSERT INTO USERSPRIME (id, name)
VALUES (
    (SELECT MAX(id) + 1 FROM USERSPRIME), 
    'test'
);

DELETE FROM USERSPRIME
WHERE id >= (SELECT AVG(id) FROM USERSPRIME as S WHERE S.id >= USERSPRIME.id);
