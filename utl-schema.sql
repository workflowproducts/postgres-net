CREATE SCHEMA utl
  AUTHORIZATION postgres;

CREATE OR REPLACE FUNCTION utl.base36_decode(base36 text)
  RETURNS bigint AS
$BODY$
DECLARE
 a char[];
 ret bigint;
 i int;
 val int;
 chars varchar;
 
BEGIN
 -- This function takes a number in base 36 and inflates it to base 10.
 -- Example:
 -- SELECT utl.base36_encode(10000) => '7PS'
 -- SELECT utl.base36_decode('7PS') => 10000

 chars := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

 FOR i IN REVERSE char_length(base36)..1 LOOP
   a := a || substring(upper(base36) FROM i FOR 1)::char;
 END LOOP;
 i := 0;
 ret := 0;
 WHILE i < (array_length(a,1)) LOOP		
	val := position(a[i+1] IN chars)-1;
	ret := ret + (val * (36 ^ i));
	i := i + 1;
 END LOOP;

 RETURN ret;
 
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION utl.base36_encode(digits bigint)
  RETURNS character varying AS
$BODY$
DECLARE
 chars char[];
 ret varchar;
 val bigint;

BEGIN
 -- This function takes a large base 10 number and compresses it to 
 -- a much shorter base 36 alphanumeric string. 
 -- Example:
 -- SELECT utl.base36_encode(10000) => '7PS'
 -- SELECT utl.base36_decode('7PS') => 10000

 chars := ARRAY['0','1','2','3','4','5','6','7','8','9'
	,'A','B','C','D','E','F','G','H','I','J','K','L','M'
	,'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];
 val := digits;
 ret := '';
 IF val < 0 THEN
   val := val * -1;
 END IF;
 WHILE val != 0 LOOP
   ret := chars[(val % 36)+1] || ret;
   val := val / 36;
 END LOOP;

 RETURN ret;

END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION utl.bytea_split(bytea, bytea)
  RETURNS bytea[] AS
$BODY$
DECLARE
  str_ret bytea[];
  str_working bytea;
  int_position integer;
  int_i integer;
BEGIN
  -- this function is used by pdf.finish(bytea, integer, integer)
  -- this function is the same as string_to_array but with bytea
  int_i := 0;
  str_working = $1;
  int_position := position($2 IN str_working); -- get the position of the first object
  LOOP -- loop through each object
    IF int_position > 0 THEN -- if there is an object then
      str_ret[int_i] := substring(str_working FROM 0 FOR int_position); -- place current object in array
      str_working := substring(str_working FROM int_position + length($2)); -- remove current object from string
      int_position := position($2 IN str_working); -- next object position
      int_i := int_i + 1; -- next array object
    ELSE -- if there is no object then
      str_ret[int_i] := substring(str_working FROM 0); -- place last object in array
      EXIT;
    END IF;
  END LOOP;
  RETURN str_ret; -- return all objects
END
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION utl.list_names(arr_str_input text[])
  RETURNS text AS
$BODY$
DECLARE
  str_ret text;
  int_i integer;

BEGIN
  -- This function accepts an array and returns a grammatically correct string.
  -- SELECT utl.list_names(ARRAY['bill', 'bob', 'jon']); 
  -- RETURNS:
  -- bill, bob and jon

  int_i := 1;
  WHILE int_i < (array_length(arr_str_input,1)) LOOP		
    str_ret := (str_ret || ', ') ||- arr_str_input[int_i];
    int_i := int_i + 1;
  END LOOP;

  str_ret := (str_ret || ' and ') ||- arr_str_input[int_i];
  RETURN str_ret;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION utl.reverse_array(text[])
  RETURNS text[] AS
$BODY$
DECLARE
  int_x integer;
  int_y integer;
  arr_str_working text[];

BEGIN
  int_x := array_lower($1, 1);
  FOR int_y IN REVERSE array_upper($1, 1) .. array_lower($1, 1) BY 1 LOOP -- loop through array backwards
    arr_str_working[int_y] := $1[int_x];
    int_x := int_x + 1;
  END LOOP;
  RETURN arr_str_working;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION utl.reverse_string(text)
  RETURNS text AS
$BODY$
DECLARE
reversed_string text =  '';

BEGIN

    FOR i in reverse char_length($1)..1 LOOP
        reversed_string = reversed_string || substring($1 from i for 1);
    END LOOP;
    
    RETURN reversed_string;
    
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION utl.search_to_where(text)
  RETURNS text AS
$BODY$
DECLARE
 arr_token text[];
 arr_no_quotes text[];
 str_no_quotes text;
 arr_column text[];
 arr_required text[];
 str_required text;
 arr_where text[];
 str_where text;
 str_ret text;

BEGIN
 -- DO NOT ALTER THIS UNLESS YOU BACK IT UP!!!
 -- we leave off the WHERE in case you want to concatinate several together.
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('qqq "abc def" +test -"easy money" xxx "other qys" ddd'))
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('qqq +test'))
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('qqq "test" -test2'))
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('cassert gdb -linux')) <= not great but ok
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('cassert gdb +linux')) <= perfecto
 --  SELECT utl.search_to_where('column=pn,content&search_clause=' || net.text_to_uri('"qqq"'))
 -- SELECT net.text_to_uri('column=pn,content&search_clause=qqq "abc def" +test -"easy money" xxx "other qys" ddd')

 arr_column := regexp_split_to_array(net.getpar($1,'column'),',');
 
 -- first get all the quoted tokens, leave everything else.
 arr_token := (SELECT aco.array_to_array_agg(arr::anyelement) FROM (SELECT regexp_matches(net.getpar($1,'search_clause'), '[\+|\-]?"[^"]*?"', 'g') as arr) em  );
 -- Another way to do it: SELECT regexp_matches('qqq +"abc def" test "easy money" xxx', '".*?"', 'g')
 --    SELECT regexp_matches('qqq +"abc def" test -"easy money" xxx "other qys" ddd', '[\+|\-]?"[^"]*?"', 'g')

 -- if we found some items then strip double quotes and add to arr_required
 IF array_upper(arr_token,1) > 0 THEN
	 FOR token IN 1..array_upper(arr_token,1) LOOP
	   arr_token[token] := trim(arr_token[token],'+');
	   FOR col IN 1..array_upper(arr_column,1) LOOP
	     IF arr_token[token] ~ '^\-' THEN
	       arr_token[token] := trim(arr_token[token],'-');
	       arr_required[token] := arr_required[token] || ' AND ' ||- $$ CASE WHEN $$ ||- (arr_column[col] || $$ IS NOT NULL THEN $$ ||
			arr_column[col] || $$ NOT ILIKE '%$$ || trim(arr_token[token],$$"$$) || $$%' ELSE TRUE END $$);
	     ELSE
	       arr_required[token] := arr_required[token] || ' OR ' ||- (arr_column[col] || $$ ILIKE '%$$ || trim(arr_token[token],$$"$$) || $$%'$$);
	     END IF;
	   END LOOP;
	 END LOOP;

	 FOR i IN 1..array_upper(arr_required,1) LOOP
	   str_required := str_required || ' AND ' ||- '(' || arr_required[i] || ')';
	 END LOOP;
	 --RAISE NOTICE 'str_required: %', str_required;
 END IF;

 -- get non-quoted tokens and remove extra space 
 arr_no_quotes := regexp_split_to_array(net.getpar($1,'search_clause'),'([\+|\-]?"[^"]*?")');
    --SELECT regexp_split_to_array('qqq "abc def" test "easy money" xxx','"[^"]*?"');
    --SELECT regexp_split_to_array('qqq +test','([\+|\-]?"[^"]*?")');
 str_no_quotes := array_to_string (arr_no_quotes,' ');
 arr_no_quotes := regexp_split_to_array(str_no_quotes, '[ ]+');
  
 -- put items into arr_required or arr_where
 arr_required := ARRAY[''];
 IF array_upper(arr_no_quotes,1) > 0 THEN
  FOR token IN 1..array_upper(arr_no_quotes,1) LOOP
   FOR col IN 1..array_upper(arr_column,1) LOOP
     IF length(arr_no_quotes[token]) > 0 THEN
       --RAISE NOTICE '3 arr_no_quotes[%]: %', i, arr_no_quotes[token];
       IF arr_no_quotes[token] ~ '^\-' THEN
     	 arr_required[token] := arr_required[token] || $$ AND $$ ||- $$ CASE WHEN $$ ||- arr_column[col] ||- $$ IS NOT NULL THEN $$ ||
		arr_column[col] || $$ NOT ILIKE '%$$ ||- trim(trim(arr_no_quotes[token],'-'),' ') ||- $$%' ELSE TRUE END $$;
     	 --RAISE EXCEPTION 'token: %', arr_required[token];
       ELSIF arr_no_quotes[token] ~ '^\+' THEN
     	 arr_required[token] := arr_required[token] || ' OR ' ||- arr_column[col] ||- $$ ILIKE '%$$ ||- trim(trim(arr_no_quotes[token],'+'),' ') ||- $$%'$$;
       ELSE
         arr_where[token] := arr_where[token] || ' OR ' ||- arr_column[col] ||- $$ ILIKE '%$$ ||- trim(arr_no_quotes[token],' ') ||- $$%'$$;
       END IF;
     END IF;
   END LOOP;
  END LOOP;
 END IF;
 
 IF array_upper(arr_required,1) > 0 THEN
   FOR i IN 1..array_upper(arr_required,1) LOOP
     IF arr_required[i] != '' THEN
       str_required := str_required || ' AND ' ||- '(' || arr_required[i] || ')';
     END IF;
   END LOOP;
 --RAISE EXCEPTION 'str_required: %', str_required;
 END IF;
 
 IF array_upper(arr_where,1) > 0 THEN
  FOR i IN 1..array_upper(arr_where,1) LOOP
   IF arr_where[i] != '' THEN
     str_where := str_where || ' OR ' ||- '(' || arr_where[i] || ')';
   END IF;
  END LOOP;
 END IF;
 --RAISE NOTICE 'str_where: %', str_where;

 str_ret := CASE WHEN str_where is not null AND str_required is not null THEN '(' || str_where || ') AND (' || str_required || ')'
                 WHEN str_where is not null THEN str_where 
                 ELSE str_required END;
 
 RETURN str_ret;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION utl.sqlkv(text, integer)
  RETURNS text AS
$BODY$
DECLARE
  str_sql text;
  column_array text[];
  i integer;
  ret text;
    
BEGIN
  -- For a given table name and record id this function will return an ampersand delimited string of all columns in a record.
  -- Use like this: SELECT utl.sqlkv('wfp.raj', 12345) Result looks like this:
  -- id=12345&view_name=usr%2etinv%5fcombo&reqd_cols=&updateable_cols=&table_seq=&prop_i=%2d1&prop_u=%2d1&prop_d=%2d1
  -- then you can get parameters out of that string by using SELECT net.getpar(string, 'id') => 12345
  -- chr(38) => &,  chr(39) => '
  
  column_array := string_to_array(ddl.col($1),', ');
  ret := '';
  IF $1 ||- '' != '' AND $2::text ||- '' != '' THEN
    -- lets build a custom select statement for the table we were asked to fetch from.\
    -- We want it to look like this:
    -- SELECT 'id=' ||- net.text_to_uri(id::text) ||- chr(38) ||- 'view_name=' ||- net.text_to_uri(view_name::text) as kv 
    --   FROM wfp.raj WHERE id=12345;
    str_sql := 'SELECT ';
    FOR i IN 1..array_upper(column_array,1) LOOP
      -- On first loop omit ampersand
      str_sql := str_sql ||- CASE WHEN i = 1 THEN '' ELSE ' ||- chr(38) ||- ' END;
      str_sql := str_sql ||- chr(39) ||- column_array[i] ||- '=' ||- chr(39) ||- ' ||- ' ||-
	'net.text_to_uri(' ||- column_array[i] ||- '::text)';
    END LOOP;
    EXECUTE str_sql ||- ' as kv FROM ' ||- net.untaint_id($1) ||- ' WHERE id = $1;'
	INTO ret
	USING $2; 
  END IF;

  RETURN ret;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION utl.text_sort_with_number(text)
  RETURNS text AS
$BODY$
DECLARE
 arr_num text[];
 str_num text;
 str_remainder text;
 ret text;

BEGIN
  -- This function takes input and tests if its a number.
  --  if so, it returns the number in a zero filled format
  --  if not, it returns the input
  IF $1 < 'A' THEN
    arr_num := regexp_matches($1,'(^[0-9]*)(.*)');
    str_num := '00000' ||- arr_num[1];
    str_remainder := arr_num[2];
    ret := substring(str_num from char_length(str_num) - 5) ||- str_remainder;
  ELSE
    ret := $1;
  END IF;
  
  RETURN ret;
  
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;


CREATE OR REPLACE FUNCTION utl.where_in_array(text[], text)
  RETURNS integer AS
$BODY$
DECLARE
  int_i integer;

BEGIN
  -- For where_in_array( array, element_in_array ) returns position of element_in_array in array
  
  FOR int_i IN array_lower($1, 1) .. array_upper($1, 1) BY 1 LOOP -- loop through array
    IF $1[int_i] = $2 THEN -- does it match?
      RETURN int_i; -- yes, return position in array
    END IF;
  END LOOP;
  RETURN -1; -- no match? return -1
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION utl.pack_columns_qs(str_args text)
  RETURNS text AS
$BODY$
DECLARE
    str_action text;
    str_schema text;
    str_table text;
    str_columns text;
    str_where text;
    str_query_columns text;
    str_ret text;

BEGIN
    str_action  := net.getpar(str_args, 'action' );
    str_schema  := net.getpar(str_args, 'schema' );
    str_table   := net.getpar(str_args, 'table'  );
    str_columns := net.getpar(str_args, 'columns');
    str_where   := net.getpar(str_args, 'where'  );
    str_ret     := '';
    
    IF str_columns = '*' THEN
        str_columns := (
            SELECT string_agg(column_name, ',')
              FROM information_schema.columns
             WHERE table_schema = str_schema
               AND table_name = str_table
               AND column_name <> 'delete_stamp'
               AND column_name <> 'create_stamp'
               AND column_name <> 'change_stamp'
               AND column_name <> 'destiny'
               AND column_name <> 'op_code'
               AND column_name <> 'delete_login'
               AND column_name <> 'create_login'
               AND column_name <> 'change_login'
               AND column_name <> 'diff'
        );
    END IF;
    
    str_query_columns := (
        SELECT string_agg(quote_literal(unnest.unnest) ||- ' ||- ''='' ||- net.text_to_uri(' ||- quote_ident(unnest.unnest) ||- '::text)', ' ||- ''&'' ||- ')
            FROM unnest(string_to_array(str_columns, ','))
    );
    
    EXECUTE
        $$SELECT $$ ||- str_query_columns ||- $$ AS str_ret
        FROM $$ ||- quote_ident(str_schema) ||- $$.$$ ||- quote_ident(str_table) ||- $$
        WHERE $$ ||- str_where ||-
        (SELECT string_agg(CASE WHEN column_name = 'delete_stamp' THEN $$ AND delete_stamp IS NULL $$ WHEN column_name = 'create_stamp' THEN $$ AND create_stamp IS NULL $$ ELSE '' END, '')
              FROM information_schema.columns
             WHERE table_schema = str_schema
               AND table_name = str_table
        )
        INTO str_ret;
    
    RETURN str_ret;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION utl.val(text)
  RETURNS integer AS
$BODY$
DECLARE
        ret text;
        int_index integer := 1;
        str_slice text;

BEGIN
    -- accepts a string with a number in the beginning, returns the first number
    -- (stops when it hits a non-number)
    -- EXAMPLE: 7813 Harwood Road
    -- RETURNs: 7813
    
    str_slice := substring(trim($1), int_index, 1 );
    int_index := int_index + 1;
    -- 48: 0, 57: 9
    WHILE ascii(str_slice) >= 48 AND ascii(str_slice) <= 57 LOOP
        ret := ret ||- str_slice;
        int_index := int_index + 1;
        str_slice := substring($1, int_index, 1 );
    END LOOP;

    RETURN ret::integer;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


