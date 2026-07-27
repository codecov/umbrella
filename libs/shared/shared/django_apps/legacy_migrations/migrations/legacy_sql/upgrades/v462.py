# v4.6.2
def run_sql(schema_editor):
    schema_editor.execute(
        """
        create or replace function commits_update_heads() returns trigger as $$
        begin

            if new.pullid is not null and new.merged is not true then
            -- update head of pulls
            update pulls p
                set updatestamp = now(),
                    head = case when head is not null
                                and (select timestamp > new.timestamp
                                    from commits c
                                    where c.repoid=new.repoid
                                    and c.commitid=p.head
                                    and c.deleted is not true
                                    limit 1)
                                then head
                                else new.commitid
                                end,
                    author = coalesce(author, new.author)
                where repoid = new.repoid
                and pullid = new.pullid;

            end if;

            -- update head of branches
            if new.branch is not null then
            update branches
                set updatestamp = now(),
                    authors = array_append_unique(coalesce(authors, '{}'::int[]), new.author),
                    head = case
                        when head is null then new.commitid
                        when (
                            head != new.commitid
                            and new.timestamp >= coalesce((select timestamp
                                                            from commits
                                                            where commitid=head
                                                            and deleted is not true
                                                            and repoid=new.repoid
                                                            limit 1), '-infinity'::timestamp)
                        ) then new.commitid
                        else head end
                where repoid = new.repoid
                and branch = new.branch;
            if not found then
                begin
                    insert into branches (repoid, updatestamp, branch, head, authors)
                    values (new.repoid, new.timestamp, new.branch, new.commitid,
                            case when new.author is not null then array[new.author] else null end);
                exception when unique_violation then
                end;
            end if;
            end if;

            return null;
        end;
        $$ language plpgsql;
    """
    )