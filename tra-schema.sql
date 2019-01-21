-- DROP SCHEMA tra;

CREATE SCHEMA tra
  AUTHORIZATION postgres;

GRANT USAGE,CREATE ON SCHEMA tra TO postgres;
GRANT USAGE ON SCHEMA tra TO trusted_g;

-- DROP FUNCTION tra.install_tra(str_args text);

CREATE OR REPLACE FUNCTION tra.install_tra(str_args text)
  RETURNS text AS
$BODY$
DECLARE
  select_cols text;
  str_table_name text;
  str_only_table_name text;
  str_sql text;
  str_trigger text;
  str_update text;
  
BEGIN
    --Usage: DO $$ EXECUTE tra.install_tra('public.rtable_test'); $$;
    
    -- INSTALL tra Tables: 
    
    --get schema and table name
    str_table_name := str_args;
    RAISE NOTICE 'str_table_name: %', str_table_name;

    --get table name
    str_only_table_name := SUBSTRING(str_table_name FROM POSITION('.' IN str_table_name) + 1);
    RAISE NOTICE 'str_only_table_name: %', str_only_table_name;
	
	--get list of columns
	select_cols := (SELECT string_agg(attname, ',' ORDER BY attnum)
	    FROM pg_catalog.pg_attribute
	    LEFT JOIN pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
	    LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
	    WHERE attname NOT IN ('change_stamp'
            , 'create_stamp'
            , 'delete_stamp'
            , 'create_login'
            , 'change_login')
        AND pg_namespace.nspname || '.' || pg_class.relname = str_table_name
        AND pg_attribute.attnum >= 0
        AND substring(pg_attribute.attname FROM 1 FOR 1) != $$.$$);
	RAISE NOTICE 'select_cols: %', select_cols;
	
	--SQL for creating table with current data
    str_sql := $$SELECT nextval('tra.tra_seq') AS pk, 'INSERT'::varchar(6) AS tg_op, CASE WHEN change_stamp = create_stamp THEN change_login ELSE 'postgres'::name END AS tg_login, 
        create_stamp AS tg_stamp, $$ || select_cols || ' INTO tra.' || str_only_table_name ||
	' FROM ' || str_table_name || ';';
    RAISE NOTICE 'str_sql: %', str_sql;
    
    --SQL for creating trigger
    str_trigger := $$--DROP TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') || $$ ON $$ || str_table_name || E';\r\n' ||
     $$CREATE TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') ||
     $$  BEFORE INSERT OR UPDATE OR DELETE ON $$ || str_table_name ||
     $$  FOR EACH ROW EXECUTE PROCEDURE tra.update_tra(); $$ || E'\r\n\r\n';
    RAISE NOTICE 'str_trigger: %', str_trigger;
    
    -- NEED TO ADD ONE UPDATE TRANSACTION PER RECORD THAT HAS BEEN UPDATED
    str_update :=  $$INSERT INTO tra.$$ || str_only_table_name || $$ (tg_op, tg_login, tg_stamp, $$ || select_cols ||
	    $$) SELECT 'UPDATE', change_login, change_stamp, $$ || select_cols ||  
	    $$ FROM $$ || str_table_name ||
	    $$ WHERE change_stamp != create_stamp;$$;
	RAISE NOTICE 'str_update: %', str_update;

    --return sql
    RETURN str_sql || E'\r\n' || str_update ||  E'\r\n' || str_trigger;

  --***********************************
  -- how tra works:
  --***********************************
  -- we are recording all transactions. 'OLD' on delete, 'NEW' on update and insert. 
  -- Using this format makes it really easy to get a snapshot of the database at 
  -- point in time.

  --***********************************
  -- what are we recording:
  --***********************************
  -- we don't insert change stamp and change login into the tra table. 
  -- These columns are redundant. change_stamp is tg_stamp. tg_login is change_login
  -- We add tg_stamp and tg_login because they are important and may not be provided by the table.

  --***********************************
  -- example single record lookup:
  --***********************************
  -- SELECT * FROM tra.rinv WHERE id = 10892
  -- pk		tg_op	tg_login	tg_stamp	id	pn		qpb	qob	trans recorded
  -- 5758	INSERT	rocket_user1	2012-03-19	10892	NAS1101-02-5	100	10	NEW
  -- 5760	UPDATE	rocket_user2	2012-03-20	10892	NAS1101-02-5	100	20	NEW
  -- 5762	UPDATE	rocket_user3	2012-03-21	10892	NAS1101-02-5	100	30	NEW
  -- 5764	DELETE	rocket_user4	2012-03-22	10892	NAS1101-02-5	100	30	OLD

  -- The first record was added on insert using NEW. Updates are recorded using NEW. Deletes are 
  -- recorded using OLD. When looking at one transaction you can see what the state of a record
  -- was when it was changed by looking up one record. For example, in 5762, we see that ten bags
  -- were added by user 3, and that when that occurred, there were twenty bags. 
  -- A point in time query would look like:
  -- SELECT *
  -- FROM tra.rinv
  -- WHERE tg_op != 'DELETE' AND pk IN (SELECT max(pk)
  --	          FROM (SELECT * 
  --		        FROM tra.rinv 
  --		        WHERE id = 10892 AND tg_stamp <= '2012-03-21') em)
  -- That's a lot easier than trying to run a query that requires old data from the tra table and
  -- current data from the usr table.

  --***********************************
  -- example point in time table:
  --***********************************
  -- in this query we are first excluding everything but the latest version of each record (by grouping on id)
  -- that is before our cutoff date. The last step is to exclude any deleted records. We now have
  -- a version of our table that looks like it did at the point in time specified.

  -- ALTERNATE EXAMPLE: use an INNER JOIN instead of a "pk IN"
  -- SELECT rquote.*
  --  FROM tra.rquote
  --  INNER JOIN (SELECT max(pk) as pk FROM tra.rquote WHERE rquote.tg_stamp < current_date GROUP BY id) qte_max ON rquote.pk = qte_max.pk
  --  WHERE tg_op != 'DELETE'
  --  ORDER BY id
  -- SELECT ddl.oid_to_columns(ddl.fullname_to_oid('usr.rcust_header'),'tra')
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;

ALTER FUNCTION tra.install_tra(str_args text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tra.install_tra(str_args text) TO postgres;
REVOKE ALL ON FUNCTION tra.install_tra(str_args text) FROM public;

--SELECT tra.install_tra(str_args text);

-- DROP FUNCTION tra.update_tra();

CREATE OR REPLACE FUNCTION tra.update_tra()
  RETURNS trigger AS
$BODY$
DECLARE
    str_table_name name;
    insert_cols text;
    value_cols text;

BEGIN
    --get current table name
    str_table_name := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
    
    --get column list
    insert_cols := (SELECT string_agg(attname, ',' ORDER BY attnum)
        FROM pg_catalog.pg_attribute
        LEFT JOIN pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
        LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE attname NOT IN ('change_stamp'
            , 'create_stamp'
            , 'delete_stamp'
            , 'create_login'
            , 'change_login')
        AND pg_namespace.nspname || '.' || pg_class.relname = str_table_name
        AND pg_attribute.attnum >= 0
        AND substring(pg_attribute.attname FROM 1 FOR 1) != $$.$$);
    RAISE NOTICE 'insert_cols: %', insert_cols;
    
    --get column list in format for getting value
    value_cols := (SELECT string_agg('$4.' || attname, ',' ORDER BY attnum)
        FROM pg_catalog.pg_attribute
        LEFT JOIN pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
        LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE attname NOT IN ('change_stamp'
            , 'create_stamp'
            , 'delete_stamp'
            , 'create_login'
            , 'change_login')
        AND pg_namespace.nspname || '.' || pg_class.relname = str_table_name
        AND pg_attribute.attnum >= 0
        AND substring(pg_attribute.attname FROM 1 FOR 1) != $$.$$);
    RAISE NOTICE 'value_cols: %', value_cols;
    
    --insert record into tra table
    EXECUTE 'INSERT INTO tra.' || TG_TABLE_NAME || ' (tg_op, tg_login, tg_stamp, ' || insert_cols || ') ' ||
        'VALUES ($1, $2, $3, ' || value_cols || ');'
    USING TG_OP, SESSION_USER, CURRENT_TIMESTAMP, CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    
    --return
    IF TG_OP = 'DELETE' THEN 
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;

ALTER FUNCTION tra.update_tra() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tra.update_tra() TO postgres;
REVOKE ALL ON FUNCTION tra.update_tra() FROM public;


CREATE SEQUENCE tra.tra_seq;
