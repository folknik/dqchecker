select
    count(1)
from {schema}.{table}
where
    {ts_field} between '{start_dt}' and '{end_dt}';