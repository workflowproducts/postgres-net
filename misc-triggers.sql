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

