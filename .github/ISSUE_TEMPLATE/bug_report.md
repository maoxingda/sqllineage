**SQL**
```sql
insert into
    public.tgt_tbl1
(
    name
)
select
    sq.name
from
    (
        select
            id,
            name
        from
            public.src_tbl1
    ) as sq
;
```

**To Reproduce**


*Note here we refer to SQL provided in prior step as stored in a file named `test.sql`*
```python
from sqllineage.runner import LineageRunner
with open("test.sql") as f:
    sql = f.read()
lr = LineageRunner(sql, dialect="redshift")
lr.draw()
```

**Expected behavior**
no public.src_tbl1.id -> sq.id

**Python version (available via `python --version`)**
 - 3.10

**SQLLineage version (available via `sqllineage --version`):**
 - 1.4.9

**Additional context**
