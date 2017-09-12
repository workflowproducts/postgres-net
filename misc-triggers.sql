CREATE OR REPLACE FUNCTION adm.default_stamp_fn()
  RETURNS trigger AS
$BODY$ 
DECLARE
  
BEGIN
  -- you may wish to replace "session_user()" with "current_user()" or a custom function depending on 
  --   your permission model
  -- we truncate timestamps to the second because many clients and front end software can't handle
  --   millisecond accuracy. For example, Microsoft Access will disallow updates if a column is 
  --   too accurate for it to handle because it will try to verify that the column didn't change
  --   with a remembered value truncated to the second.
  IF TG_OP = 'INSERT' THEN
    NEW.create_login := "session_user"();
    NEW.create_stamp := date_trunc('second',now());
  END IF;
  NEW.change_login := "session_user"();
  NEW.change_stamp := date_trunc('second',now());
  RETURN NEW; 
END; 

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION usr.update_tra()
  RETURNS trigger AS
$BODY$
DECLARE
  tbl_name name;
  insert_cols text;
  value_cols text;
  
BEGIN
  --   CREATE TRIGGER tra_trg_rinv
  --   BEFORE INSERT OR UPDATE OR DELETE
  --   ON usr.rinv
  --   FOR EACH ROW
  --   EXECUTE PROCEDURE usr.update_tra();

  --***********************************
  -- concerning how tra works:
  --***********************************

  -- we are recording all transactions. 'OLD' on delete, 'NEW' on update and insert. 
  -- Using this format makes it really easy to get a snapshot of the database at 
  -- point in time. 

  -- example of looking up the history of one record in database table:
  -- SELECT * FROM tra.rinv WHERE id = 11734;
  -- pk		tg_op	  tg_login	    tg_stamp	  id	  pn		        qpb	qob	trans recorded
  -- 5758	INSERT	rocket_user1	2012-03-19	11734	NAS1101-02-X	100	10	NEW
  -- 5760	UPDATE	rocket_user2	2012-03-20	11734	NAS1101-02-X	100	20	NEW
  -- 5762	UPDATE	rocket_user3	2012-03-21	11734	NAS1101-02-X	100	30	NEW
  -- 5764	DELETE	rocket_user4	2012-03-22	11734	NAS1101-02-X	100	30	OLD

  -- The first record was added on insert using NEW. Updates are recorded using NEW. Deletes are 
  -- recorded using OLD. When looking at one transaction you can see what the state of a record
  -- was when it was changed by looking up one record. For example, in 5762, we see that ten qob
  -- were added by rocket_user3, and that when that occurred, there were twenty bags. 
  -- A point in time query would look like:
--   SELECT *
--   FROM tra.rinv
--   WHERE tg_op != 'DELETE' AND pk in (SELECT max(pk)
--   	          FROM (SELECT * 
--   		            FROM tra.rinv 
--   		            WHERE id = 11734 AND tg_stamp <= '2014-03-21') em)
-- NOTE: The result for this example will be the latest record version before the date given.
--       
  -- That's a lot easier than trying to run a query that requires old data from the tra table and
  -- current data from the usr table.
  

  tbl_name := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
  insert_cols := (SELECT ddl.oid_to_columns(ddl.fullname_to_oid(tbl_name),'tra'));
  value_cols := (SELECT ddl.oid_to_columns(ddl.fullname_to_oid(tbl_name),'tra_array'));

  --RAISE NOTICE '%', 'INSERT INTO tra.' || TG_TABLE_NAME || ' (tg_op, tg_login, tg_stamp, ' || insert_cols || ') ' ||
	--'VALUES (' || tg_op || ', ' || session_user || ', ' || current_timestamp || ', ' || value_cols || ');';

  EXECUTE 'INSERT INTO tra.' || TG_TABLE_NAME || ' (tg_op, tg_login, tg_stamp, ' || insert_cols || ') ' ||
	'VALUES ($1, $2, $3, ' || value_cols || ');'
    USING TG_OP, SESSION_USER, CURRENT_TIMESTAMP, CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;

  IF TG_OP = 'DELETE' THEN 
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;

ALTER FUNCTION usr.update_tra() OWNER TO postgres;
