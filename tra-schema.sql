-- DROP SCHEMA tra;

CREATE SCHEMA tra
  AUTHORIZATION postgres;

GRANT USAGE,CREATE ON SCHEMA tra TO postgres;

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
  str_alter text;
  
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
     $$  FOR EACH ROW EXECUTE PROCEDURE tra.update_tra(); $$;
    RAISE NOTICE 'str_trigger: %', str_trigger;
    
    -- NEED TO ADD ONE UPDATE TRANSACTION PER RECORD THAT HAS BEEN UPDATED
    str_update :=  $$INSERT INTO tra.$$ || str_only_table_name || $$ (pk, tg_op, tg_login, tg_stamp, $$ || select_cols ||
	    $$) SELECT nextval('tra.tra_seq') AS pk, 'UPDATE', change_login, change_stamp, $$ || select_cols ||  
	    $$ FROM $$ || str_table_name ||
	    $$ WHERE change_stamp != create_stamp;$$;
	RAISE NOTICE 'str_update: %', str_update;

    str_alter := $$ALTER TABLE tra.$$ || str_only_table_name || $$ ALTER COLUMN pk SET DEFAULT nextval('tra.tra_seq');$$;

    --return sql
    RETURN str_sql || E'\r\n' || str_update ||  E'\r\n' || str_trigger || E'\r\n' || str_alter || E'\r\n\r\n';

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

-- DROP FUNCTION tra.tra_check(text);

CREATE OR REPLACE FUNCTION tra.tra_check(text)
  RETURNS text AS
$BODY$
--send schema as argument
--checks for missing tables, columns, or incorrect column types
SELECT string_agg(em.result, E'\n') AS result
FROM (
    SELECT E'Missing TRA columns:\n'
        || string_agg(
            pg_namespace.nspname || '.' || pg_class.relname || '.' || pg_attribute.attname
            , E'\n'
            ORDER BY pg_namespace.nspname || '.' || pg_class.relname || '.' || pg_attribute.attname
        )
        || E'\n\n' AS result
    FROM pg_catalog.pg_namespace 
    LEFT JOIN pg_catalog.pg_class ON pg_class.relnamespace = pg_namespace.oid
    LEFT JOIN pg_catalog.pg_attribute ON pg_attribute.attrelid = pg_class.oid
    LEFT JOIN pg_catalog.pg_namespace tra_pg_namespace ON TRUE
    LEFT JOIN pg_catalog.pg_class tra_pg_class ON tra_pg_class.relnamespace = tra_pg_namespace.oid AND pg_class.relname = tra_pg_class.relname
    LEFT JOIN pg_catalog.pg_attribute tra_pg_attribute ON tra_pg_attribute.attrelid = tra_pg_class.oid AND pg_attribute.attname = tra_pg_attribute.attname
    WHERE pg_namespace.nspname = $1
    AND tra_pg_namespace.nspname = 'tra'
   AND pg_class.relname ILIKE 'rclient'
    AND tra_pg_class.relname IS NOT NULL
    
    AND tra_pg_attribute.attname IS NULL
    
    AND pg_class.relkind = 'r'
    AND tra_pg_class.relkind = 'r'
    AND pg_attribute.attname NOT LIKE '%pg.dropped%'
    AND pg_attribute.attname NOT IN (
        'ctid'
        , 'xmin'
        , 'cmin'
        , 'xmax'
        , 'cmax'
        , 'tableoid'
        
        , 'change_login'
        , 'change_stamp'
        , 'create_stamp'
    )
    UNION
    SELECT E'TRA table type mismatch:\n'
        || string_agg(
            pg_namespace.nspname || '.' || pg_class.relname || '.' || pg_attribute.attname
            , E'\n'
            ORDER BY pg_namespace.nspname || '.' || pg_class.relname || '.' || pg_attribute.attname
        )
        || E'\n\n' AS result
    FROM pg_catalog.pg_namespace 
    LEFT JOIN pg_catalog.pg_class ON pg_class.relnamespace = pg_namespace.oid
    LEFT JOIN pg_catalog.pg_attribute ON pg_attribute.attrelid = pg_class.oid
    LEFT JOIN pg_catalog.pg_namespace tra_pg_namespace ON TRUE
    LEFT JOIN pg_catalog.pg_class tra_pg_class ON tra_pg_class.relnamespace = tra_pg_namespace.oid AND pg_class.relname = tra_pg_class.relname
    LEFT JOIN pg_catalog.pg_attribute tra_pg_attribute ON tra_pg_attribute.attrelid = tra_pg_class.oid AND pg_attribute.attname = tra_pg_attribute.attname
    WHERE pg_namespace.nspname = $1
    AND tra_pg_namespace.nspname = 'tra'
    AND pg_class.relname ILIKE 'rclient'
    AND tra_pg_class.relname IS NOT NULL
    
    AND tra_pg_attribute.attname IS NOT NULL
    AND (
        format_type(tra_pg_attribute.atttypid, tra_pg_attribute.atttypmod) IS DISTINCT FROM format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
    )
    
    AND pg_class.relkind = 'r'
    AND tra_pg_class.relkind = 'r'
    AND pg_attribute.attname NOT LIKE '%pg.dropped%'
    AND pg_attribute.attname NOT IN (
        'ctid'
        , 'xmin'
        , 'cmin'
        , 'xmax'
        , 'cmax'
        , 'tableoid'
        
        , 'change_login'
        , 'change_stamp'
        , 'create_stamp'
    )
    UNION
    SELECT E'Missing TRA table:\n'
        || string_agg(
            pg_namespace.nspname ||- '.' ||- pg_class.relname
            , E'\n'
            ORDER BY pg_namespace.nspname ||- '.' ||- pg_class.relname ASC
        )
        || E'\n\n' AS result
    FROM pg_catalog.pg_class
    LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE pg_namespace.nspname = $1
    AND relkind = 'r'
    AND pg_class.relname NOT IN (
        SELECT pg_class.relname
        FROM pg_catalog.pg_class
        LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = 'tra'
        AND relkind = 'r'
    )
    

) em
;
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

ALTER FUNCTION tra.tra_check(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tra.tra_check(text) TO postgres;
GRANT EXECUTE ON FUNCTION tra.tra_check(text) TO public;

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



/*
-- to fix NULL pks (because of older versions of this schema)

--recommended you do this after hours, when nothing will be updated while you do this

--run sql generated by column 1 to create the pk column (if necessary)
--run sql generated by column 2 to update the pk column (only does 100 records at a time, keep running until you get all zero's)
--run sql generated by column 3 to alter the pk column (to use tra.tra_seq)
SELECT 'ALTER TABLE ' ||- pg_namespace.nspname ||- '.' ||- pg_class.relname ||- E' ADD COLUMN pk integer;' AS create_column
    , REPLACE($SQL$UPDATE {{FULLNAME}}
SET pk = em.new_pk
FROM (
    SELECT (SELECT COALESCE(max(pk), 0) AS max_pk FROM {{FULLNAME}})
        + row_number() OVER (ORDER BY tg_stamp ASC, id ASC) AS new_pk, *
    FROM {{FULLNAME}}
    WHERE pk IS NULL
    ORDER BY tg_stamp ASC, id ASC
    LIMIT 1000
) em
WHERE $SQL$ ||- string_agg('em.' ||- pg_attribute.attname ||- ' IS NOT DISTINCT FROM ' ||- pg_class.relname ||- '.' ||- pg_attribute.attname
            , E'\nAND ' ORDER BY attnum ASC) ||- ';'
        , '{{FULLNAME}}', pg_namespace.nspname ||- '.' ||- pg_class.relname) AS fill_pk_column
    , 'ALTER TABLE ' ||- pg_namespace.nspname ||- '.' ||- pg_class.relname ||- E' ALTER COLUMN pk SET DEFAULT nextval(\'tra.tra_seq\');' AS alter_pk_set_default
FROM pg_catalog.pg_class
LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
LEFT JOIN pg_catalog.pg_attribute ON pg_attribute.attrelid = pg_class.oid
WHERE pg_namespace.nspname = 'tra'
AND pg_class.relkind = 'r'
AND pg_attribute.attnum > 0
AND pg_attribute.attisdropped IS FALSE
GROUP BY pg_namespace.nspname, pg_class.relname
ORDER BY pg_namespace.nspname ASC, pg_class.relname ASC;

--run to generate sql, to generate sql, to fix sequence
SELECT $SQL$SELECT 'ALTER SEQUENCE tra.tra_seq RESTART WITH ' ||- (max(max_pk) + 1)::text ||- ';'
FROM ($SQL$
    ||- string_agg(
    REPLACE($SQL$SELECT max(pk) AS max_pk FROM {{FULLNAME}} $SQL$, '{{FULLNAME}}', pg_namespace.nspname ||- '.' ||- pg_class.relname)
    , E'\nUNION\n' ORDER BY pg_namespace.nspname ASC, pg_class.relname ASC) ||- $SQL$) em;$SQL$ AS use_to_set_sequence
FROM pg_catalog.pg_class
LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE pg_namespace.nspname = 'tra'
AND pg_class.relkind = 'r';

--run the update column sql (column 2) again just to be sure you get all zeros

*/




/*
-- to fix NULL pks ALTERNATE (because of older versions of this schema)
DO $$
DECLARE
  c CURSOR FOR
    SELECT *
    FROM tra.rtesting_table
    WHERE pk IS NULL
    ORDER BY rtesting_table.tg_stamp ASC, id ASC
    FOR UPDATE;
BEGIN
  FOR row IN c LOOP
    UPDATE tra.rtesting_table
    SET pk = nextval('tra.tra_seq')
    WHERE CURRENT OF c;
  END LOOP;
END
$$;
*/

/*
What follows is an alternate set of functions that operate on STATEMENT level TRIGGERs.
The usage is very similar to the above functions but due to the way that
PostgreSQL STATEMENT TRIGGERs work, they need three triggers per table in
order to work. One each for INSERT,UPDATE,DELETE.
*/

-- DROP FUNCTION tra.install_tra_statement(str_args text);

CREATE OR REPLACE FUNCTION tra.install_tra_statement(str_args text)
  RETURNS text AS
$BODY$
DECLARE
  select_cols text;
  str_table_name text;
  str_only_table_name text;
  str_sql text;
  str_trigger text;
  str_update text;
  str_alter text;
  
BEGIN
    --Usage: DO $$ EXECUTE tra.install_tra_statement('public.rtable_test'); $$;
    
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
    str_sql := $$SELECT nextval('tra.tra_seq') AS tg_pk, 'INSERT'::varchar(6) AS tg_op, CASE WHEN change_stamp = create_stamp THEN change_login ELSE 'postgres'::name END AS tg_login, 
        create_stamp AS tg_stamp, $$ || select_cols || ' INTO tra.' || str_only_table_name ||
	' FROM ' || str_table_name || ';';
    RAISE NOTICE 'str_sql: %', str_sql;
    
    --SQL for creating trigger
    str_trigger := $$--DROP TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') || $$ ON $$ || str_table_name || E';\r\n' ||
     $$CREATE TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') || $$_insert AFTER INSERT ON $$ || str_table_name || $$ REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE PROCEDURE tra.update_tra_statement(); $$ || E';\r\n' ||
     $$CREATE TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') || $$_update AFTER UPDATE ON $$ || str_table_name || $$ REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE PROCEDURE tra.update_tra_statement(); $$ || E';\r\n' ||
     $$CREATE TRIGGER tra_trg_$$ || replace(str_table_name,'.','_') || $$_delete AFTER DELETE ON $$ || str_table_name || $$ REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT EXECUTE PROCEDURE tra.update_tra_statement(); $$;
     
    RAISE NOTICE 'str_trigger: %', str_trigger;
    
    -- NEED TO ADD ONE UPDATE TRANSACTION PER RECORD THAT HAS BEEN UPDATED
    str_update :=  $$INSERT INTO tra.$$ || str_only_table_name || $$ (tg_pk, tg_op, tg_login, tg_stamp, $$ || select_cols ||
	    $$) SELECT nextval('tra.tra_seq') AS tg_pk, 'UPDATE', change_login, change_stamp, $$ || select_cols ||  
	    $$ FROM $$ || str_table_name ||
	    $$ WHERE change_stamp != create_stamp;$$;
	RAISE NOTICE 'str_update: %', str_update;

    str_alter := $$ALTER TABLE tra.$$ || str_only_table_name || $$ ALTER COLUMN tg_pk SET DEFAULT nextval('tra.tra_seq');$$;

    --return sql
    RETURN str_sql || E'\r\n' || str_update ||  E'\r\n' || str_trigger || E'\r\n' || str_alter || E'\r\n\r\n';

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

ALTER FUNCTION tra.install_tra_statement(str_args text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tra.install_tra_statement(str_args text) TO postgres;
REVOKE ALL ON FUNCTION tra.install_tra_statement(str_args text) FROM public;

--SELECT tra.install_tra_statement(str_args text);

-- DROP FUNCTION tra.update_tra_statement();

CREATE OR REPLACE FUNCTION tra.update_tra_statement()
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
    -- RAISE NOTICE 'insert_cols: %', insert_cols;
    
    --get column list in format for getting value
    value_cols := (SELECT string_agg(CASE WHEN TG_OP = 'DELETE' THEN 'deleted' ELSE 'inserted' END || '.' || attname, ',' ORDER BY attnum)
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
    -- RAISE NOTICE 'value_cols: %', value_cols;
    
    --insert record into tra table
    EXECUTE 'INSERT INTO tra.' || TG_TABLE_NAME || ' (tg_op, tg_login, tg_stamp, ' || insert_cols || ') ' ||    
        'SELECT $1, $2, $3, ' || value_cols || ' FROM ' || CASE WHEN TG_OP = 'DELETE' THEN 'deleted' ELSE 'inserted' END
    USING TG_OP, SESSION_USER, CURRENT_TIMESTAMP;
    
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

ALTER FUNCTION tra.update_tra_statement() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tra.update_tra_statement() TO postgres;
REVOKE ALL ON FUNCTION tra.update_tra_statement() FROM public;
