(**
Here are the constructs allowed in expression position for where, select etc. 
They are translated to Hive query expression constructs, see the table in the Apache docs [link].

There are some bugs in the translation of these, they have not all been tested. For example Replace doesn't seem to be recognized by Hive. If you spot problems please either 
fix them or submit a pull request to update the notes column.

| F# expression consutrct |   Hive query cosntruct     |    Note     |
|-------------------------|----------------------------|-------------|
|  s.Length               |    LENGTH(s)               |             |
|  s.StartsWith(arg)      |    s LIKE(arg%)            |             |
|  s.EndsWith(arg)        |    s LIKE(%arg)            |             |
|  s.Contains(arg)        |    s LIKE(%arg%)           |             |
|  s.ToUpper()            |    UPPER(s)                |             |
|  s.ToLower()            |    LOWER(s)                |             |
| System.String.IsNullOrEmpty(s) | s IS NULL OR s=""   |             |
|  s.Replace("a","b")     |    REPLACE(s,'a','b')      | Not working |
|  s.Substring(start)     |    SUBSTRING(s, start, 8000)  |             | 
|  s.Substring(start, length) | SUBSTRING(s, start, length) |           | 
|  s.Remove(start)        |    STUFF(s, start, 8000)    |              |
|  s.Remove(start, length) |   STUFF(s, start, length)  |              |
|  s.IndexOf(arg)         |   CHARINDEX(arg, s)         |              |
|  s.IndexOf(arg, start)  |   CHARINDEX(arg, s, start)  |              |
|  s.Trim                 |   RTRIM(LTRIM(s))           |              |
|  s.TrimEnd              |   RTRIM(s)                  |              |
|  s.TrimStart            |   LTRIM(s)                  |              |
| a + b                   |   a + b                     |              |
| a - b                   |   a - b                     |              |
| a * b                   |   a * b                     |              |
| a / b                   |   a / b                     |              |
| a % b                   |   a % b                     |              |
| -a                      |   -a                        |              | 
| a ** b                  |   POWER(a,b)                |              |
| ceil x                  |  CEILING(x)                 |              |
| floor x                 |  FLOOR(x)                   |              |
| round x                 |  ROUND(x)                   |              |
| System.Decimal.Round(x, numberOfDecimalsToRoundTo) |  ROUND(x)                   |              |
| truncate x              |  ROUND(x)                   |              |
| abs x | ABS(x) ||
| acos x | ACOS(x) ||
| asin x | ASIN(x) ||
| atan x | ATAN(x) ||
| cos x | COS(x) ||
| exp x | EXP(x) ||
| log10 x | LOG10(x) ||
| sin x | SIN(x) ||
| tan x | TAN(x) ||
| sqrt x | SQRT(x) ||
| sign x | SIGN(x) ||
| atan2 x | ATAN2(x) ||
| log2 x | LOG2(x) ||

*)