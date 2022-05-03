CREATE SCHEMA dte
  AUTHORIZATION postgres;

CREATE OR REPLACE FUNCTION dte.focy()
  RETURNS text AS
$BODY$
 -- returns first day of current year
 SELECT '01-01-' || EXTRACT(year FROM current_date);
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION dte.focy() OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.focy(text)
  RETURNS text AS
$BODY$
DECLARE
focy text;
BEGIN
   -- returns first day of supplied year or current year if null
   IF $1 is null then
     SELECT '01-01-' || EXTRACT(year FROM current_date) INTO focy;
   else
     SELECT '01-01-' || EXTRACT(year FROM $1::date) INTO focy;
   end if;
   RETURN focy;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.focy(text) OWNER TO postgres;

CREATE OR REPLACE FUNCTION dte.fom()
  RETURNS date AS
$BODY$
    -- returns first of month
    SELECT (EXTRACT(month FROM current_date)::integer  || '-01-' || EXTRACT(year FROM current_date)::integer)::date;
$BODY$
LANGUAGE sql VOLATILE COST 100;

ALTER FUNCTION dte.fom() OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.fom(text)
  RETURNS date AS
$BODY$
DECLARE
    fom text;
BEGIN
    -- returns first of supplied month or current month.
    IF $1 is null THEN
        SELECT (EXTRACT(month FROM current_date)::integer || '-01-' || EXTRACT(year FROM current_date)::integer)::date INTO fom;
    ELSE
        SELECT (EXTRACT(month FROM $1::date)::integer || '-01-' || EXTRACT(year FROM $1::date)::integer)::date INTO fom;
    END IF;
  RETURN fom;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.fom(text) OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.foty()
  RETURNS text AS
$BODY$
    -- returns first of year.
    SELECT '04-01-' || EXTRACT(year FROM current_date);
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION dte.foty() OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.foty(text)
  RETURNS text AS
$BODY$
DECLARE
    foty text;
BEGIN
    -- returns first day of the year
    IF $1 is null THEN
        SELECT '04-01-' || EXTRACT(year FROM current_date) INTO foty;
    ELSE
        SELECT '04-01-' || EXTRACT(year FROM $1::date) INTO foty;
    END IF;
   RETURN foty;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.foty(text) OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.get_period(text)
  RETURNS date AS
$BODY$
DECLARE
  cmonth integer;
  cday integer;
  cyear integer;
  rmonth integer;
  rday integer;
  ryear integer;
  ret date;

BEGIN
  -- this one is odd. Assuming the first part of a month is a 'period' and the last 
  --   half of the month is a 'period', accept various descriptions about which period
  --   we want returned and then return the first date of the one we want.
  SELECT EXTRACT(month FROM current_date)::integer INTO cmonth;
  SELECT EXTRACT(day FROM current_date)::integer INTO cday;
  SELECT EXTRACT(year FROM current_date)::integer INTO cyear;

  IF $1 = 'begin current period' THEN
    SELECT CASE WHEN cday <= 15 THEN 1 ELSE 16 END INTO rday;
    ryear := cyear;
    rmonth := cmonth;
  
  ELSIF $1 = 'end current period' THEN
    SELECT CASE WHEN cday <= 15 THEN 15 ELSE EXTRACT(day from dte.lom()) END INTO rday;
    ryear := cyear;
    rmonth := cmonth;

  ELSIF $1 = 'begin last period' THEN
    SELECT CASE WHEN cday <= 15 AND cmonth = 1 THEN 12 ELSE cmonth - 1 END INTO rmonth;
    SELECT CASE WHEN cday <= 15 AND cmonth = 1 THEN cyear - 1 ELSE cyear END INTO ryear;
    SELECT CASE WHEN cday <= 15 THEN 16 ELSE 1 END INTO rday;
    
  ELSIF $1 = 'end last period' THEN
    SELECT CASE WHEN cday <= 15 AND cmonth = 1 THEN 12 ELSE cmonth - 1 END INTO rmonth;
    SELECT CASE WHEN cday <= 15 AND cmonth = 1 THEN cyear - 1 ELSE cyear END INTO ryear;
    SELECT CASE WHEN cday <= 15 THEN EXTRACT(day from (dte.lom(ryear || '-' || rmonth || '-' || cday))) ELSE 15 END INTO rday;

  END IF;

  ret := (ryear || '-' || rmonth || '-' || rday)::date;
  RETURN ret;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.get_period(text) OWNER TO postgres;



CREATE OR REPLACE FUNCTION dte.getquarter()
  RETURNS integer AS
$BODY$
   SELECT EXTRACT(quarter FROM current_date)::integer;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION dte.getquarter() OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.getquarter(text)
  RETURNS text AS
$BODY$
DECLARE
qtr text;
BEGIN
  IF $1 is null then
    SELECT EXTRACT(quarter FROM current_date)::integer INTO qtr;
  else
    SELECT EXTRACT(quarter FROM $1::date)::integer INTO qtr;
  end if;
  RETURN qtr;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.getquarter(text) OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.lom()
  RETURNS date AS
$BODY$

DECLARE
lom text;

BEGIN
    -- returns last day of previous month.
    SELECT ((EXTRACT(month FROM current_date)::integer
      || '-01-' || EXTRACT(year FROM current_date)::integer)::date + '1 month'::interval) - '1 day'::interval INTO lom;

  RETURN lom;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.lom() OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.lom(text)
  RETURNS date AS
$BODY$

DECLARE
    lom text;

BEGIN
    -- accepts null or date. returns last day of previous month.
  IF $1 is null then 
    SELECT ((EXTRACT(month FROM current_date)::integer
      || '-01-' || EXTRACT(year FROM current_date)::integer)::date + '1 month'::interval) - '1 day'::interval INTO lom;
  else
    SELECT ((EXTRACT(month FROM $1::date)::integer
      || '-01-' || EXTRACT(year FROM $1::date)::integer)::date + '1 month'::interval) - '1 day'::interval INTO lom;
  end if;
  RETURN lom;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.lom(text) OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.previous_date_of_day(the_date date, dow integer)
  RETURNS date AS
$BODY$
    SELECT CASE WHEN extract(dow from $1) < $2 THEN
        $1 - ( extract(dow from $1) + (7 - $2) )::int
    ELSE
        $1 - ( extract(dow from $1) - $2)::int
    END;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION dte.previous_date_of_day(the_date date, dow integer) OWNER TO postgres;


CREATE OR REPLACE FUNCTION dte.timeclock_noround(time without time zone)
  RETURNS numeric AS
$BODY$
    -- SELECT dte.timeclock_noround(current_timestamp::time without time zone)
    -- used to convert time to numeric field.
  SELECT (((EXTRACT(HOUR FROM $1)*60*60)+(EXTRACT(MINUTE FROM $1)*60)+EXTRACT(SECOND FROM $1))/60/60)::numeric(15,2);
$BODY$
  LANGUAGE SQL VOLATILE
  COST 100;

ALTER FUNCTION dte.timeclock_noround(time without time zone) OWNER TO postgres;



CREATE OR REPLACE FUNCTION dte.for_month_and_day_give_next_future_date(date)
  RETURNS integer AS
$BODY$
DECLARE
  cur_date date := current_date;
  evt_date date := $1::date;

BEGIN
    -- this function accepts any past or future date and replaces the year with 
    --  this year if the day has not occurred yet, or next year if it has;
    --  then it figures out how many days until that day and 
    --  returns the number of days to that day.
    RETURN (CASE WHEN (
                date_part('month', evt_date) || '-' || 
                date_part('day', evt_date) || '-' ||
                date_part('year', cur_date) )::date - cur_date >= 0
                
        THEN (  date_part('month', evt_date) || '-' || 
                date_part('day', evt_date) || '-' ||
                date_part('year', cur_date) )::date - cur_date
                
        ELSE (  date_part('month', evt_date) || '-' || 
                date_part('day', evt_date) || '-' ||
                date_part('year', cur_date + '1 year'::interval) )::date - cur_date END);

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

ALTER FUNCTION dte.for_month_and_day_give_next_future_date(date) OWNER TO postgres;

CREATE OR REPLACE FUNCTION dte.add_business_day(dte_from_date date, int_days integer)
  RETURNS date AS
$BODY$
SELECT COALESCE(
    (
        SELECT em2.em1
        FROM (
            SELECT em1.em1::date
                , row_number() OVER (ORDER BY CASE WHEN int_days = abs(int_days) THEN em1.em1 END, em1.em1 DESC)
            FROM generate_series(
                dte_from_date + CASE WHEN int_days = abs(int_days) THEN 1 ELSE -1 END
                , dte_from_date + (((abs(int_days) * 2) + 5) * CASE WHEN int_days = abs(int_days) THEN 1 ELSE -1 END)
                , '1 day'::interval * CASE WHEN int_days = abs(int_days) THEN 1 ELSE -1 END
            ) em1
            WHERE EXTRACT('dow' FROM em1.em1) NOT IN (0, 6)
        ) em2
        WHERE row_number = abs(int_days)
    )
, dte_from_date)
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

ALTER FUNCTION dte.add_business_day(dte_from_date date, int_days integer) OWNER TO postgres;
