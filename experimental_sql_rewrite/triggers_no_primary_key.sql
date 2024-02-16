DROP VIEW IF EXISTS usersprime;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS usersplus;
DROP TABLE IF EXISTS usersminus;

CREATE TABLE IF NOT EXISTS users (
    id        INT
);

CREATE TABLE IF NOT EXISTS usersplus (
    id        INT
);

CREATE TABLE IF NOT EXISTS usersminus (
    id        INT
);

INSERT INTO users VALUES (1);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
INSERT INTO users VALUES (2);


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
 RAISE NOTICE 'Trigger redirect_insert executed for ID %', NEW.id; 
INSERT INTO usersplus VALUES (NEW.id);
RETURN NEW;
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
  RAISE NOTICE 'Trigger redirect_update executed for NEW ID % OLD ID % ', NEW.id, OLD.id;
  INSERT INTO usersminus (id) VALUES (OLD.id);
  INSERT INTO usersplus (id) VALUES (NEW.id);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_update_trigger
  INSTEAD OF UPDATE ON USERSPRIME
  FOR EACH ROW
  EXECUTE PROCEDURE redirect_update();

-- examples
update usersprime set id=id+1;
