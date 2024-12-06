DROP VIEW IF EXISTS usertable cascade;
ALTER TABLE usertableprod RENAME TO usertable;
DROP TABLE IF EXISTS usertableplus;
DROP TABLE IF EXISTS usertableminus;
DELETE FROM usertable;
