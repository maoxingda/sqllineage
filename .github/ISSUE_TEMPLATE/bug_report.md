**SQL**
```sql
insert into public.tgt_tbl1
(
    name,
    email
)
select
    st1.name,
    st1.name || st1.email || '@gmail.com' as email
from
    public.src_tbl1 as st1
```

**To Reproduce**


*Note here we refer to SQL provided in prior step as stored in a file named `test.sql`*
```python
from sqllineage.runner import LineageRunner

with open("test.sql") as f:
    sql = f.read()

lr = LineageRunner(sql, dialect="redshift")

lr.print_column_lineage()
```

**Actual behavior**
```
public.tgt_tbl1.email <- public.src_tbl1.email
```

**Expected behavior**
```
public.tgt_tbl1.name <- public.src_tbl1.name
public.tgt_tbl1.name <- public.src_tbl1.email
public.tgt_tbl1.email <- public.src_tbl1.email
```

**Python version (available via `python --version`)**
 - 3.10

**SQLLineage version (available via `sqllineage --version`):**
 - 1.5.0

**Additional context**
