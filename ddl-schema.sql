DROP FUNCTION ddl.writeable(str_folder text);
DROP FUNCTION ddl.table_to_view(text, text, text);
DROP FUNCTION ddl.rename_table(text, text);
DROP FUNCTION ddl.refresh_table_view(text, text, text);
DROP FUNCTION ddl.readable(str_username text, str_folder text);
DROP FUNCTION ddl.readable(str_folder text);
DROP FUNCTION ddl.oid_to_schema(oid);
DROP FUNCTION ddl.oid_to_name(oid);
DROP FUNCTION ddl.oid_to_fullname(oid);
DROP FUNCTION ddl.name_to_fullname(text);
DROP FUNCTION ddl.groups_user(name);
DROP FUNCTION ddl.groups_user();
DROP FUNCTION ddl.group_user(name, name);
DROP FUNCTION ddl.group_user(name);
DROP FUNCTION ddl.function_stored(text);
DROP FUNCTION ddl.col(text);
DROP FUNCTION ddl.fullname_to_oid(text);
DROP FUNCTION ddl.schema_to_oid(text);
DROP FUNCTION ddl.oid_to_columns(oid, text);
DROP SCHEMA ddl;


-- DROP SCHEMA ddl;

CREATE SCHEMA ddl
  AUTHORIZATION postgres;



-- DROP FUNCTION ddl.oid_to_columns(oid, text);

CREATE OR REPLACE FUNCTION ddl.oid_to_columns(oid, text)
  RETURNS text AS
$BODY$
DECLARE
    tbl text;
    sch text;
    column_names text;
    column_array text[];
    pk_array integer[];
    i integer;
    pk_columns text := '';
    nonpk_columns text := '';
    update_columns text := '';
    new_columns text := '';
    view_columns text := '';
    noid_columns text := '';
    prop_columns text := '';
    text_columns text := '';
    raj_cols text;
    column_type text;
    column_type_array text[];
    tra_columns text;
    tra_array_columns text;
    
    --Args: table oid, 'update|pk|nonpk|all(default)'
BEGIN
    tbl := ddl.oid_to_name($1);
    sch := ddl.oid_to_schema($1);
    
    SELECT string_agg(quote_ident(attname::text), ', ')
        , string_agg(atttypid::text, ', ')
    INTO column_names, column_type
    FROM pg_class c 
    JOIN pg_attribute a ON c.oid = a.attrelid 
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relname = tbl
        AND a.attnum >= 0
        AND n.nspname = sch
        AND substring(attname from 1 for 1) != $$.$$;

    column_array := string_to_array(column_names, ', ');
    column_type_array := string_to_array(column_type, ', ');
    
    SELECT pg_constraint.conkey
    INTO pk_array
    FROM pg_class
    INNER JOIN pg_constraint ON pg_constraint.conrelid = pg_class.oid
    WHERE pg_class.oid = $1
        AND pg_constraint.contype = 'p';

    FOR i IN array_lower(column_array,1)..array_upper(column_array,1) LOOP
        IF column_array[i] != 'port_number' THEN
            text_columns := text_columns || ',' || (column_array[i] || '::text');
            raj_cols := raj_cols || ' || $$,$$ || ' ||
                        CASE WHEN column_type_array[i] = '114' 
                             THEN 'COALESCE(' || column_array[i] || $$::text,'null')$$ 
                             ELSE 'net.jsonify(' || column_array[i] || ')'
                        END;
        END IF;
        
        --RAISE NOTICE '%: %', i, column_array[i];

        -- all columns but change_stamp, create_stamp, create_login, change_login
        -- if view type, the change stamp column is allowed
        IF (column_array[i] != 'change_stamp' OR $2 = 'view')
                AND column_array[i] != 'create_stamp'
                AND column_array[i] != 'delete_stamp'
                AND column_array[i] != 'create_login'
                AND column_array[i] != 'change_login' THEN
            view_columns := view_columns || ',' || column_array[i];
            tra_columns := tra_columns || ',' || column_array[i];
            tra_array_columns := tra_array_columns || ',' || ('$4.' || column_array[i]);
        END IF;
        --RAISE NOTICE '%: %', i, view_columns;

        IF i = ANY (pk_array) THEN
            pk_columns := pk_columns || ',' || column_array[i];
        ELSE
            noid_columns := noid_columns || ',' || column_array[i];
            prop_columns := prop_columns || ',' || 'cat_prop(' || column_array[i] || ') AS ' || column_array[i];
            -- all columns but change_stamp, create_stamp, create_login, change_login
            -- if view type, the change stamp column is allowed
            IF (column_array[i] != 'change_stamp' OR $2 = 'view')
                    AND column_array[i] != 'create_stamp'
                    AND column_array[i] != 'delete_stamp'
                    AND column_array[i] != 'create_login'
                    AND column_array[i] != 'change_login' THEN
                nonpk_columns := nonpk_columns || ',' || column_array[i];
                new_columns := new_columns || ',' || 'new.' || column_array[i];
                update_columns := update_columns || ',' || column_array[i] || '=new.' || column_array[i];
            END IF;
        END IF;
    END LOOP;
    
    RETURN regexp_replace(CASE WHEN $2 = 'update'      THEN update_columns 
                WHEN $2 = 'pk'          THEN pk_columns 
                WHEN $2 = 'nonpk'       THEN nonpk_columns
                WHEN $2 = 'new'         THEN new_columns
                WHEN $2 = 'view'        THEN view_columns
                WHEN $2 = 'noid'        THEN noid_columns
                WHEN $2 = 'prop'        THEN prop_columns
                WHEN $2 = 'text'        THEN text_columns
                WHEN $2 = 'cat text'    THEN raj_cols
                WHEN $2 = 'tra'         THEN tra_columns
                WHEN $2 = 'tra_array'   THEN tra_array_columns	
                ELSE column_names END, '^,', '');
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION ddl.oid_to_columns(oid, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION ddl.oid_to_columns(oid, text) TO postgres;
REVOKE ALL ON FUNCTION ddl.oid_to_columns(oid, text) FROM public;

--SELECT ddl.oid_to_columns(oid, text);


-- DROP FUNCTION ddl.schema_to_oid(text);

CREATE OR REPLACE FUNCTION ddl.schema_to_oid(text)
  RETURNS oid AS
$BODY$
  SELECT oid::integer FROM pg_namespace WHERE nspname = $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.schema_to_oid(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.schema_to_oid(text) FROM public;

--SELECT ddl.schema_to_oid(text);


-- DROP FUNCTION ddl.fullname_to_oid(text);

CREATE OR REPLACE FUNCTION ddl.fullname_to_oid(text)
  RETURNS oid AS
$BODY$
SELECT oid FROM pg_class WHERE relname = substring($1 from position('.' in $1)+1)
	AND relnamespace=ddl.schema_to_oid(substring($1 from 1 for position('.' in $1)-1));
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

ALTER FUNCTION ddl.fullname_to_oid(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.fullname_to_oid(text) FROM public;

--SELECT ddl.fullname_to_oid(text);


-- DROP FUNCTION ddl.col(text);

CREATE OR REPLACE FUNCTION ddl.col(text)
  RETURNS text AS
$BODY$
   SELECT ddl.oid_to_columns(ddl.fullname_to_oid($1),'');
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

ALTER FUNCTION ddl.col(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.col(text) FROM public;

--SELECT ddl.col(text);


-- DROP FUNCTION ddl.function_stored(text);

CREATE OR REPLACE FUNCTION ddl.function_stored(text)
  RETURNS text AS
$BODY$
  SELECT prosrc FROM pg_proc WHERE proname = $1;
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

ALTER FUNCTION ddl.function_stored(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.function_stored(text) FROM public;

--SELECT ddl.function_stored(text);


-- DROP FUNCTION ddl.group_user(name);

CREATE OR REPLACE FUNCTION ddl.group_user(name)
  RETURNS boolean AS
$BODY$

  -- accepts group name, returns whether session user has permission to that group
  SELECT CASE WHEN (SELECT count(*)
	FROM pg_roles r
	JOIN pg_auth_members ON r.oid=roleid
	JOIN pg_roles u ON member = u.oid
	WHERE r.rolname = $1 AND u.rolname = session_user) > 0 THEN true ELSE false END;
	
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.group_user(name) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.group_user(name) FROM public;

--SELECT ddl.group_user(name);


-- DROP FUNCTION ddl.group_user(name, name);

CREATE OR REPLACE FUNCTION ddl.group_user(name, name)
  RETURNS boolean AS
$BODY$

  -- accepts (user name, group name), returns whether user has permission to that group
  SELECT CASE WHEN (SELECT count(*)
	FROM pg_roles r
	JOIN pg_auth_members ON r.oid=roleid
	JOIN pg_roles u ON member = u.oid
	WHERE r.rolname = $2 AND u.rolname = $1 ) > 0 THEN true ELSE false END;
	
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.group_user(name, name) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.group_user(name, name) FROM public;

--SELECT ddl.group_user(name, name);


-- DROP FUNCTION ddl.groups_user();

CREATE OR REPLACE FUNCTION ddl.groups_user()
  RETURNS text AS
$BODY$

  -- returns a comma seperated list of groups the session user belongs to. 
  SELECT string_agg(r.rolname, ', ')::text
	FROM pg_roles r
	JOIN pg_auth_members ON r.oid=roleid
	JOIN pg_roles u ON member = u.oid
	WHERE u.rolname = session_user;

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.groups_user() OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.groups_user() FROM public;

--SELECT ddl.groups_user();


-- DROP FUNCTION ddl.groups_user(name);

CREATE OR REPLACE FUNCTION ddl.groups_user(name)
  RETURNS text AS
$BODY$

  -- accepts user name, returns a comma seperated list of groups the user belongs to. 
  SELECT string_agg(r.rolname, ', ')::text
	FROM pg_roles r
	JOIN pg_auth_members ON r.oid=roleid
	JOIN pg_roles u ON member = u.oid
	WHERE u.rolname = $1;
	
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.groups_user(name) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.groups_user(name) FROM public;

--SELECT ddl.groups_user(name);


-- DROP FUNCTION ddl.name_to_fullname(text);

CREATE OR REPLACE FUNCTION ddl.name_to_fullname(text)
  RETURNS text AS
$BODY$
DECLARE
 tbl text;
 sch text;
 dup_chk integer;

BEGIN
 IF position('.' in $1) > 0 THEN
  tbl := substring($1 from position('.' in $1)+1);
  sch := substring($1 from 1 for position('.' in $1)-1);
  RETURN sch || '.' || tbl;
 ELSE
  tbl := $1;
  SELECT INTO dup_chk sum(1) FROM pg_tables 
    WHERE tablename=tbl AND schemaname !~ 'pg_|information_schema|pgagent';
  IF dup_chk != 1 THEN
    RAISE NOTICE 'There are % tables named "%".',dup_chk, tbl;
    RETURN null;
  ELSE
    SELECT INTO sch schemaname FROM pg_tables 
	WHERE tablename = tbl;
    RETURN sch || '.' || tbl;
  END IF;
 END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION ddl.name_to_fullname(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.name_to_fullname(text) FROM public;

--SELECT ddl.name_to_fullname(text);


-- DROP FUNCTION ddl.oid_to_fullname(oid);

CREATE OR REPLACE FUNCTION ddl.oid_to_fullname(oid)
  RETURNS text AS
$BODY$
  SELECT nspname || '.' || relname FROM pg_class 
  INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
  WHERE pg_class.oid = $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.oid_to_fullname(oid) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.oid_to_fullname(oid) FROM public;

--SELECT ddl.oid_to_fullname(oid);


-- DROP FUNCTION ddl.oid_to_name(oid);

CREATE OR REPLACE FUNCTION ddl.oid_to_name(oid)
  RETURNS text AS
$BODY$
  SELECT relname::text FROM pg_class 
  INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
  WHERE pg_class.oid = $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.oid_to_name(oid) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.oid_to_name(oid) FROM public;

--SELECT ddl.oid_to_name(oid);


-- DROP FUNCTION ddl.oid_to_schema(oid);

CREATE OR REPLACE FUNCTION ddl.oid_to_schema(oid)
  RETURNS text AS
$BODY$
SELECT nspname::text FROM pg_class 
  INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
  WHERE pg_class.oid = $1 OR pg_namespace.oid=$1;
$BODY$
  LANGUAGE sql IMMUTABLE
  COST 100;

ALTER FUNCTION ddl.oid_to_schema(oid) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.oid_to_schema(oid) FROM public;

--SELECT ddl.oid_to_schema(oid);


-- DROP FUNCTION ddl.readable(str_folder text);

CREATE OR REPLACE FUNCTION ddl.readable(str_folder text)
  RETURNS boolean AS
$BODY$
  SELECT ddl.group_user('developer_g') OR
         (SESSION_USER = lower(str_folder)) OR
         ('all' = str_folder) OR
         ddl.group_user(lower(str_folder));
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.readable(str_folder text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.readable(str_folder text) FROM public;

--SELECT ddl.readable(str_folder text);


-- DROP FUNCTION ddl.readable(str_username text, str_folder text);

CREATE OR REPLACE FUNCTION ddl.readable(str_username text, str_folder text)
  RETURNS boolean AS
$BODY$
  SELECT ddl.group_user(str_username, 'developer_g') OR
         (str_username = str_folder) OR
         ('all' = str_folder) OR
         ddl.group_user(str_username, str_folder);
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.readable(str_username text, str_folder text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.readable(str_username text, str_folder text) FROM public;

--SELECT ddl.readable(str_username text, str_folder text);


-- DROP FUNCTION ddl.refresh_table_view(text, text, text);

CREATE OR REPLACE FUNCTION ddl.refresh_table_view(text, text, text)
  RETURNS text AS
$BODY$
DECLARE
  fullname text;
  toid oid;
  shortname text;
  sch text;
  view_name text;
  primary_list text[];
  primary_clause text := '';
  delete_rule text;
  insert_rule text;
  update_rule text;
  grant_statement text;
  i integer;
--Args: table name, table prefix, view prefix
  notes text;

BEGIN
  fullname := ddl.name_to_fullname($1);
  toid := ddl.fullname_to_oid(fullname);
  shortname := ddl.oid_to_name(toid);
  sch := ddl.oid_to_schema(toid);
  IF fullname IS NOT NULL THEN
    IF NOT shortname like $2 || '%' THEN
      RAISE EXCEPTION 'Table name "%" not prefixed with %',shortname,$2;
      RETURN NULL;
    ELSE
      --drop the view if it exists
      view_name := $3 || substring(shortname from length($2)+1);
      IF ddl.fullname_to_oid(sch || '.' || view_name) IS NOT NULL THEN
        EXECUTE 'DROP VIEW ' || sch || '.' || view_name || ';';
      END IF;
      -- create the view
      EXECUTE 'CREATE VIEW ' || sch || '.' || view_name || ' AS ' || 
	'SELECT ' || ddl.oid_to_columns(toid,'view') || ' FROM ' || sch || '.' || shortname;

      --construct the delete rule
      delete_rule := 'CREATE RULE ' || shortname || '_delete AS ON ' ||
        'DELETE TO ' ||  sch || '.' || view_name || ' DO INSTEAD ' || 
        'DELETE FROM ' || fullname || ' WHERE ';

      --construct primary key clause
      primary_list := string_to_array(ddl.oid_to_columns(toid, 'pk'), ', ');

      FOR i IN 1..array_upper(primary_list,1) LOOP
        primary_clause := primary_clause || CASE WHEN i > 1 THEN 'AND ' ELSE '' END || 'old.' || primary_list[i] 
          || ' = ' || shortname || '.' || primary_list[i] || ' ';
      END LOOP;
      
      EXECUTE delete_rule || primary_clause || ';';

      --construct the insert rule
      insert_rule := 'CREATE RULE ' || shortname || '_insert AS ON '
        || 'INSERT TO ' ||  sch || '.' || view_name || ' DO INSTEAD '
        || 'INSERT INTO ' || fullname || ' (' || ddl.oid_to_columns(toid,'nonpk') || ') '
        || 'VALUES (' || ddl.oid_to_columns(toid,'new') || ');';
      EXECUTE insert_rule;

      --construct the update rule
      update_rule := 'CREATE RULE ' || shortname || '_update AS ON '
        || 'UPDATE TO ' ||  sch || '.' || view_name || ' DO INSTEAD  '
        || 'UPDATE ' || fullname 
        || ' SET ' || ddl.oid_to_columns(toid,'update') || ' '
        || 'WHERE ';
       EXECUTE update_rule || primary_clause || ';';

      --construct msaccess grant statement
      --grant_statement := 'GRANT ALL ON TABLE ' ||  sch || '.' || view_name || ' TO msaccess;';
      --EXECUTE grant_statement;
      
      RETURN 'View "' || sch || '.' || view_name || '" refreshed.';
    END IF;
  ELSE
    RETURN NULL;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION ddl.refresh_table_view(text, text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION ddl.refresh_table_view(text, text, text) TO postgres;
REVOKE ALL ON FUNCTION ddl.refresh_table_view(text, text, text) FROM public;

--SELECT ddl.refresh_table_view(text, text, text);


-- DROP FUNCTION ddl.rename_table(text, text);

CREATE OR REPLACE FUNCTION ddl.rename_table(text, text)
  RETURNS text AS
$BODY$
DECLARE
  toid oid;
  fullname text;
  old_name text;
  new_name text;
--Args: table name, old prefix, new prefix

BEGIN
  fullname := ddl.name_to_fullname($1);
  IF fullname IS NOT NULL THEN
    toid := ddl.fullname_to_oid(fullname);
    EXECUTE 'ALTER TABLE ' || fullname || ' RENAME TO ' || $2;
    RETURN  ddl.oid_to_fullname(toid);
  ELSE
    RETURN NULL;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION ddl.rename_table(text, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.rename_table(text, text) FROM public;

--SELECT ddl.rename_table(text, text);


-- DROP FUNCTION ddl.table_to_view(text, text, text);

CREATE OR REPLACE FUNCTION ddl.table_to_view(text, text, text)
  RETURNS text AS
$BODY$
DECLARE
  fullname text;
  toid oid;
  shortname text;
  sch text;
  name_after_rename text;
  fullname_after_rename text;
  primary_list text[];
  primary_clause text;
  delete_rule text;
  insert_rule text;
  update_rule text;
  i integer;
--Args: table name, old prefix, new prefix

BEGIN
  fullname := ddl.name_to_fullname($1);
  toid := ddl.fullname_to_oid(fullname);
  shortname := ddl.oid_to_name(toid);
  sch := ddl.oid_to_schema(toid);
  IF fullname IS NOT NULL THEN
    IF NOT shortname like $2 || '%' THEN
      RAISE EXCEPTION 'Table name "%" not prefixed with %',shortname,$2;
      RETURN NULL;
    ELSE
      name_after_rename := $3 || substring(shortname from length($2)+1);
      fullname_after_rename := ddl.rename_table(fullname,name_after_rename);
      EXECUTE 'CREATE VIEW ' || fullname || ' AS ' || 
	'SELECT * FROM ' || sch || '.' ||name_after_rename;

      --construct the delete rule
      delete_rule := 'CREATE RULE ' || shortname || '_delete AS ON ' ||
        'DELETE TO ' ||  fullname || ' DO INSTEAD ' || 
        'DELETE FROM ' || fullname_after_rename || ' WHERE ';

      --construct primary key clause
      primary_list := string_to_array(ddl.oid_to_columns(toid, '', 'pk'), ', ');

      FOR i IN 1..array_upper(primary_list,1) LOOP
        primary_clause := primary_clause || CASE WHEN i > 1 THEN 'AND ' ELSE '' END || 'old.' || primary_list[i] 
          || ' = ' || name_after_rename || '.' || primary_list[i] || ' ';
      END LOOP;
      
      EXECUTE delete_rule || primary_clause || ';';

      --construct the insert rule
      insert_rule := 'CREATE RULE ' || shortname || '_insert AS ON '
        || 'INSERT TO ' ||  fullname || ' DO INSTEAD '
        || 'INSERT INTO ' || fullname_after_rename || ' (' || ddl.oid_to_columns(toid,'','nonpk') || ') '
        || 'VALUES (' || ddl.oid_to_columns(toid,'new.','nonpk') || ');';
      --RAISE NOTICE '%',insert_rule;
      EXECUTE insert_rule;

      --construct the update rule
      update_rule := 'CREATE RULE ' || shortname || '_update AS ON '
        || 'UPDATE TO ' ||  fullname || ' DO INSTEAD  '
        || 'UPDATE ' || fullname_after_rename 
        || ' SET ' || ddl.oid_to_columns(toid,'','update') || ' '
        || 'WHERE ';


      EXECUTE update_rule || primary_clause || ';';
      
      RETURN 'Table "' || fullname || '" renamed "' || fullname_after_rename
	|| '". View "' || fullname || '" created.';
    END IF;
  ELSE
    RETURN NULL;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION ddl.table_to_view(text, text, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.table_to_view(text, text, text) FROM public;

--SELECT ddl.table_to_view(text, text, text);


-- DROP FUNCTION ddl.writeable(str_folder text);

CREATE OR REPLACE FUNCTION ddl.writeable(str_folder text)
  RETURNS boolean AS
$BODY$
  SELECT ddl.group_user('developer_g') OR
         (SESSION_USER = lower(str_folder)) OR
         ddl.group_user(regexp_replace(lower(str_folder), '_g$', '_w'));
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION ddl.writeable(str_folder text) OWNER TO postgres;
REVOKE ALL ON FUNCTION ddl.writeable(str_folder text) FROM public;

--SELECT ddl.writeable(str_folder text);


