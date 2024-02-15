-- users as snapshot at T
CREATE TABLE IF NOT EXISTS users (
    id        INT PRIMARY KEY,
    name varchar(80)
);

CREATE TABLE IF NOT EXISTS usersplus (
    id        INT PRIMARY KEY,
    name varchar(80)
);

CREATE TABLE IF NOT EXISTS usersminus (
    id        INT PRIMARY KEY
);

INSERT INTO users VALUES (1,'user1');
INSERT INTO users VALUES (2,'user2');
INSERT INTO users VALUES (3,'user3');
INSERT INTO users VALUES (4,'user4');
INSERT INTO users VALUES (5,'user5');

-- CREATE R' as view
CREATE OR REPLACE VIEW USERSPRIME AS
		SELECT *
		FROM users
		WHERE id NOT IN (SELECT id FROM usersplus)
		AND id NOT IN (SELECT id FROM usersminus)
		UNION ALL
		SELECT * FROM usersplus;

-- INSERT 
CREATE OR REPLACE FUNCTION redirect_insert()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
 RAISE NOTICE 'Trigger redirect_insert executed for ID %', NEW.id; 
 IF EXISTS (SELECT * FROM USERSPRIME WHERE id = NEW.id) THEN
  RAISE EXCEPTION 'id already exists %', OLD.id;
 ELSE
  IF EXISTS (SELECT * FROM usersminus WHERE id = NEW.id) THEN
   DELETE FROM usersminus WHERE id=NEW.id;
  END IF;
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
 RAISE NOTICE 'Trigger redirect_delete executed for ID %', OLD.id; 
  DELETE FROM usersplus WHERE id = OLD.id;
  INSERT INTO usersminus (id) 
  VALUES (OLD.id);
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
 RAISE NOTICE 'Trigger redirect_update executed for ID %', NEW.id; 
  IF NOT EXISTS (SELECT * FROM usersplus WHERE ID = OLD.id) THEN
    INSERT INTO usersplus SELECT * FROM USERSPRIME where id=OLD.id;
  END IF;
  UPDATE usersplus SET name = NEW.name WHERE id = NEW.id;
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
