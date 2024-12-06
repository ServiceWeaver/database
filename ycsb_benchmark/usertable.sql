CREATE TABLE IF NOT EXISTS usertableplus (
     ycsb_key VARCHAR(64) NOT NULL,
     field0 VARCHAR(100),
     field1 VARCHAR(100),
     field2 VARCHAR(100),
     field3 VARCHAR(100),
     field4 VARCHAR(100),     
     field5 VARCHAR(100),
     field6 VARCHAR(100),
     field7 VARCHAR(100),
     field8 VARCHAR(100),
     field9 VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS usertableminus (
     ycsb_key VARCHAR(64) NOT NULL,
     field0 VARCHAR(100),
     field1 VARCHAR(100),
     field2 VARCHAR(100),
     field3 VARCHAR(100),
     field4 VARCHAR(100),     
     field5 VARCHAR(100),
     field6 VARCHAR(100),
     field7 VARCHAR(100),
     field8 VARCHAR(100),
     field9 VARCHAR(100)
);

-- alter original table
ALTER TABLE usertable RENAME TO usertableprod;

CREATE VIEW usertable AS
SELECT field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key FROM
( SELECT field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key FROM usertableprod
UNION ALL
SELECT field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key FROM usertableplus) AS tmp
WHERE NOT EXISTS(
SELECT 1 FROM usertableminus
WHERE (tmp.field0,tmp.field1,tmp.field2,tmp.field3,tmp.field4,tmp.field5,tmp.field6,tmp.field7,tmp.field8,tmp.field9,tmp.ycsb_key) = (usertableminus.field0,usertableminus.field1,usertableminus.field2,usertableminus.field3,usertableminus.field4,usertableminus.field5,usertableminus.field6,usertableminus.field7,usertableminus.field8,usertableminus.field9,usertableminus.ycsb_key)
);

-- insert

CREATE OR REPLACE FUNCTION redirect_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
RAISE NOTICE 'Trigger redirect_insert executed for ycsb_key %', NEW.ycsb_key; 
IF EXISTS (SELECT * FROM usertable WHERE ycsb_key = NEW.ycsb_key) THEN
     RAISE EXCEPTION 'id already exists %', OLD.ycsb_key;
ELSE
     INSERT INTO usertableplus (field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key) 
     VALUES (NEW.field0, NEW.field1, NEW.field2, NEW.field3, NEW.field4, NEW.field5, NEW.field6, NEW.field7, NEW.field8, NEW.field9, NEW.ycsb_key);
     RETURN NEW;
END IF;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_insert_trigger
     INSTEAD OF INSERT ON usertable
     FOR EACH ROW
     EXECUTE PROCEDURE redirect_insert();

-- delete
CREATE OR REPLACE FUNCTION redirect_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
RAISE NOTICE 'Trigger redirect_delete executed for ycsb_key %', OLD.ycsb_key; 
INSERT INTO usertableminus (field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key) 
VALUES (OLD.field0, OLD.field1, OLD.field2, OLD.field3, OLD.field4, OLD.field5, OLD.field6, OLD.field7, OLD.field8, OLD.field9, OLD.ycsb_key);
RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER redirect_delete_trigger
     INSTEAD OF DELETE ON usertable
     FOR EACH ROW
     EXECUTE PROCEDURE redirect_delete();

-- update
CREATE OR REPLACE FUNCTION redirect_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
RAISE NOTICE 'Trigger redirect_update executed for NEW ycsb_key % OLD ycsb_key %', NEW.ycsb_key,OLD.ycsb_key;
IF EXISTS (SELECT * FROM usertable WHERE ycsb_key = NEW.ycsb_key) THEN
     RAISE EXCEPTION 'ycsb key already exists %', OLD.ycsb_key;
END IF;
INSERT INTO usertableminus (field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key) 
VALUES (OLD.field0, OLD.field1, OLD.field2, OLD.field3, OLD.field4, OLD.field5, OLD.field6, OLD.field7, OLD.field8, OLD.field9, OLD.ycsb_key);

INSERT INTO usertableplus (field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, ycsb_key) 
VALUES (NEW.field0, NEW.field1, NEW.field2, NEW.field3, NEW.field4, NEW.field5, NEW.field6, NEW.field7, NEW.field8, NEW.field9, NEW.ycsb_key);

RETURN NEW;
END;
$$;
	
CREATE OR REPLACE TRIGGER redirect_update_trigger
     INSTEAD OF UPDATE ON usertable
     FOR EACH ROW
     EXECUTE PROCEDURE redirect_update();

