Data Stats
=========

1) Note on column types in schema:
Column types currently supported by script are
 1) integer, decimal
 2) timestamp, date
 3) string
Currently, any other datatypes in schema file will be treated as string by default

2) Note on delimiter
The delimiter of csv files is hardcoded currently to '\u2566' not reading from
 schema file

3) Note on export
Currently, the stats results are exported to shared storage(path specified
 using --exportPath). Modify the exportStats.py to change it.
