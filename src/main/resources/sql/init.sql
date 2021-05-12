create or replace function public.create_delete_trigger_if_not_exists(t_name text, tr_name text)
  returns void as
$$
  BEGIN
    IF NOT EXISTS(SELECT * FROM information_schema.triggers
                  WHERE event_object_table = t_name AND trigger_name = tr_name) THEN
      execute 'create trigger ' || tr_name || ' after delete or truncate on "' || t_name || '" for each statement execute procedure notify_delete();';
    END IF ;
  END;
$$
LANGUAGE plpgsql;

create or replace function public.create_insert_or_update_trigger_if_not_exists(t_name text, tr_name text)
  returns void as
$$
BEGIN
  IF NOT EXISTS(SELECT * FROM information_schema.triggers
                WHERE event_object_table = t_name AND trigger_name = tr_name) THEN
    execute 'create trigger ' || tr_name || ' after insert or update on "' || t_name || '" for each row execute procedure notify_insert_or_update();';
  END IF ;
END;
$$
  LANGUAGE plpgsql;

create or replace function public.notify_insert_or_update()
returns trigger as
$$
  declare
  payload text;
  op text;
  begin
    CASE TG_OP
    WHEN 'INSERT' THEN
       op := 'I';
    WHEN 'UPDATE' THEN
       op := 'U';
    ELSE
       RAISE EXCEPTION 'only support insert or update';
    END CASE;
  payload := '' || '{'
                || '"op":"'        || op                ||'",'
                || '"t":"'     || TG_TABLE_NAME     || '",'
                || '"id":'     || NEW.id
                || '}';
  execute pg_notify('mutation', payload);
  return NULL;
  end;
$$ language plpgsql;

create or replace function public.notify_delete()
returns trigger as
$$
  declare
  payload text;
  begin
  payload := '' || '{'
                || '"op":"D",'
                || '"t":"'     || TG_TABLE_NAME || '"'
                || '}';
  execute pg_notify('mutation', payload);
  return NULL;
  end;
$$ language plpgsql;
