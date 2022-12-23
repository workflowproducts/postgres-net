
CREATE SCHEMA net
	AUTHORIZATION postgres;

CREATE OR REPLACE FUNCTION net.text_to_uri(text)
	RETURNS text AS
$BODY$
DECLARE
	str_working text;
	str_working2 text;
	str_slice text;
	str_ret text;

BEGIN
	-- Takes text as input, returns uri encoded text
	-- SELECT net.text_to_uri('test 3');
	-- test%203
	str_working := $1;
	str_ret := '';
	WHILE length(str_working) > 0 LOOP
		str_slice := substring(str_working, 1, 1);
		IF str_slice ~* '[a-z]|[0-9]' THEN
			str_ret := str_ret || str_slice;
		-- deal with unicode stuffs
		ELSEIF octet_length(str_slice) > 1 THEN
		    str_working2 := encode(convert_to(str_slice::text, 'utf8')::bytea, 'hex');
		    WHILE length(str_working2) > 0 LOOP
		        str_ret := str_ret || '%' || substring(str_working2 FROM 1 FOR 2);
		        str_working2 := substring(str_working2 FROM 3);
		    END LOOP;
		-- if we don't handle this case, the bytea cast breaks
		ELSEIF str_slice = E'\\' THEN
		    str_ret := str_ret || '%5C';
		ELSE
			str_ret := str_ret || '%' || encode(str_slice::bytea,'hex');
		END IF;
		str_working := substring(str_working, 2);
	END LOOP;

	RETURN str_ret;
END;
$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;
ALTER FUNCTION net.text_to_uri(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION net.text_to_uri(text) TO public;

CREATE OR REPLACE FUNCTION net.getpar(text, text)
	RETURNS text AS
$BODY$
DECLARE
	strArray text[];
	ret text;

BEGIN
	-- Takes query string for first input, second input is the key for the value you want to extract
	-- returns value after it has been decoded using uri_to_text
	-- SELECT net.getpar('test1=value1&test2=value%202', 'test2');
	-- value 2
	strArray := string_to_array($1, '&');
	IF array_upper(strArray,1) IS NULL THEN
		RETURN NULL;
	ELSE
		for i IN 1..array_upper(strArray,1) loop
			IF split_part(strArray[i], '=', 1) = $2 THEN
				ret = substring(strArray[i] FROM position('=' in strArray[i]) + 1);
			END IF;
		end loop;
		IF ret != '' THEN
			ret := net.uri_to_text(ret);
		ELSE
			ret := NULL;
		END IF;
		RETURN ret;
	END IF;
END

$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;
ALTER FUNCTION net.getpar(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION net.getpar(text, text) TO public;

CREATE OR REPLACE FUNCTION net.getpare(text, text)
	RETURNS text AS
$BODY$
DECLARE
	strArray text[];
	ret text;

BEGIN
	-- Takes query string for first input, second input is the key for the value you want to extract
	-- returns value WITHOUT decoding using uri_to_text
	-- SELECT net.getpare('test1=value1&test2=value%202', 'test2');
	-- value%202
	strArray := string_to_array($1, '&');
	IF array_upper(strArray,1) IS NULL THEN
		RETURN NULL;
	ELSE
		for i IN 1..array_upper(strArray,1) loop
			IF split_part(strArray[i], '=', 1) = $2 THEN
				ret = substring(strArray[i] FROM position('=' in strArray[i]) + 1);
			END IF;
		end loop;
		IF ret = '' THEN
			ret := NULL;
		END IF;
		RETURN ret;
	END IF;

END

$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;

ALTER FUNCTION net.getpare(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION net.getpare(text, text) TO public;

CREATE OR REPLACE FUNCTION net.uri_to_text(text)
	RETURNS text AS
$BODY$
DECLARE
	str_working text;
	str_slice text;
	str_ret text;
	byt_ret bytea := '\x';
	int_i integer;
	int_len integer;

BEGIN
	-- Takes uri encoded text as input, returns decoded text
	-- SELECT net.uri_to_text('test%203');
	-- test 3
	str_working := replace($1, '+', ' ');
	int_i := 1;
	int_len := length(str_working);
	WHILE int_i <= int_len LOOP
		str_slice := substring(str_working from int_i for 1);
		IF str_slice = '%' THEN
			str_slice := substring(str_working from int_i + 1 for 2);
			byt_ret := byt_ret || ('\x' ||- str_slice)::bytea;
			int_i := int_i + 2;
		ELSE
			byt_ret := byt_ret || str_slice::bytea;
		END IF;
		int_i := int_i + 1;
	END LOOP;
	str_ret := convert_from(byt_ret, 'utf8');
	RETURN str_ret;
END;
$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;
ALTER FUNCTION net.uri_to_text(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION net.uri_to_text(text) TO public;

CREATE OR REPLACE FUNCTION net.jsonify(anyelement)
	RETURNS text AS
$BODY$
DECLARE
BEGIN
	-- Takes string, date, integer, etc
	-- Returns properly encoded JSON value
	-- SELECT net.jsonify('test');
	-- "test"
	RETURN rtrim(ltrim(array_to_json(ARRAY[[$1]])::text, '['), ']');
END;
$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;

ALTER FUNCTION net.jsonify(anyelement) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION net.jsonify(anyelement) TO public;
