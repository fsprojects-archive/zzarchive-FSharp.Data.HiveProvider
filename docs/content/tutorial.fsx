(*** hide ***)
#I "../../bin"

//context.DataContext.ExecuteCommand("CREATE TABLE partition_sample (code STRING, description STRING, total_emp INT) PARTITIONED BY (salary INT)")
//context.DataContext.ExecuteCommand("FROM sample_07 s INSERT INTO TABLE partition_sample PARTITION (salary=20000)
//                                    SELECT s.code, s.description, s.total_emp WHERE s.salary < 20000")
//context.partition_sample.GetSchema()
//
//hiveQuery { for row in context.sample_07 do}

(**
Tutorial: Building simple Hive queries
========================

This tutorial demonstrates different ways of running simple Hive queries 
on a Hadoop system. The samples included here use a clean installation of the
[Hortonworks Sandbox](http://hortonworks.com/products/hortonworks-sandbox/) and query
some of the sample tables included out of the box.

We first load the assembly of the type provider and open the required namespaces.
*)
#r @"../../bin/HiveTypeProvider.dll"
open Hive.HiveRuntime
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols
open Microsoft.FSharp.Linq.NullableOperators

(**
Statically typed view of existing tables
----------------------------------------

The tables (e.g. [`sample_07`](https://github.com/hortonworks/hortonworks-sandbox/blob/master/apps/beeswax/data/sample_07.csv)) 
are accessible as provided types.
*)
[<Literal>]
let dsn = "Sample Hortonworks Hive DSN; pwd=hadoop"
type Conn = Hive.HiveTypeProvider<dsn, DefaultMetadataTimeout=1000>
let context = Conn.GetDataContext()

(** This allows us to see the raw requests sent to Hadoop. *)
context.DataContext.RequestSent.Add(fun msg -> printf "REQUEST: %s" msg)
(**
The schema of each table is brought into the program as types, which can be used within a `hiveQuery` 
(similar to F# query expressions, but also supporting some Hive-specific operators). 
The types are available through IntelliSense, making it easier to write exploratory queries.
*)
let query = hiveQuery {for row in context.sample_07 do
                       where (row.salary ?< 20000)
                       select row.description}
(**
The `hiveQuery` is translated into this HiveQL query:

    [lang=sql]
    SELECT description FROM sample_07 WHERE salary < 20000

And we can now run the query in order to get the data.
*)
query.Run()

(**
In this case, null values are dealt with using [nullable
operators](http://msdn.microsoft.com/en-us/library/hh323953.aspx) like `?<`, which 
return `false` when a null value is encountered. Otherwise, calling `row.salary.Value` on
a null entry would result in a `System.InvalidOperationException`.

Static parameters of HiveTypeProvider
---------------------------------
We have used the required `dsn` paramter above in order to specify the ODBC DSN of our Hadoop deployment. Here
we will work with three other (optional) parameters.

### **UseRequiredAnnotations**
This is another way of handling null values. Although we cannot enforce non-null requirements for columns in
Hive tables, we can add a `(required)` comment to that column, which tells the Hive Provider not to expect null
values. In our very first example, hovering over `row.salary` will reveal its type as `System.Nullable<int>`.
However, if the `salary` column would be annotated, its type would be just `int` instead.

We can do this by executing a raw Hive command. _Note:_ The user needs to have 
[ALTER privileges](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Authorization#LanguageManualAuthorization-Grant/RevokePrivileges) in order for this
to work.
*)

context.DataContext.ExecuteCommand("ALTER TABLE sample_08 CHANGE salary salary int COMMENT '(required)'")
(** After executing this, close and re-open the IDE in order to make the changes to the schema available within your script.*)

 (**Note that the type of `salary` has changed and we now use regular F# operators instead of the nullable ones.*)
hiveQuery {for row in context.sample_08 do
           where (row.salary < 20000)
           select row.description }

(**The output HiveQL is identical to the one above, except it queries the `sample_08` table. *)

(** Required annotations are enabled by default, but we can pass a static parameter in order to ignore them.*)
type Conn' = Hive.HiveTypeProvider<dsn, UseRequiredAnnotations=false, DefaultMetadataTimeout=1000>
let context' = Conn'.GetDataContext()

(** 
### **DefaultQueryTimeout** 
The default query timeout can be set through this static paramter. If unused, the default value is 60s.

### **DefaultMetadataTimeout**
Can pe passed in order to set the timeout in seconds used when fetching metadata from the Hive ODBC service at compile-time, 
and the default timeout for accesses to the metadata at runtime. If left unspecified, the default value is 5s.
The second code snippet in this tutorial contains a usage example.

Custom operators in hiveQuery expressions
---------------------------------------
Some operators also have nullable counterparts, which can be used when aggregating values which we expect
to contain nulls.

### __averageBy / averageByNullable__ *)
let query2 = hiveQuery {for line in context.sample_07 do
                        where (line.salary ?< 30000)
                        averageBy (float (line.salary.Value))}
(** In the above, we can use `averageBy` instead of its nullable variant because the nullable operator in the
`where` clause filters out all the nulls, allowing us to safely use `Value` on the nullable integers.

The resulting query is

    [lang=sql]
    SELECT AVG(salary) FROM sample_07 WHERE salary < 30000 

### __sumBy / sumByNullable__ *)
let query3 = hiveQuery {for line in context.sample_07 do
                        where (line.salary ?> 50000)
                        sumByNullable (line.total_emp)}

(** 
The resulting query is

    [lang=sql]
    SELECT SUM(total_emp) FROM sample_07 WHERE salary > 50000

### __maxBy / maxByNullable / minBy / minByNullable__ *)
let query4 = hiveQuery {for line in context.sample_08 do
                        where (line.total_emp ?> 1000)
                        maxBy line.salary}
(** Here, we are using `maxBy` instead of the `maxByNullable` because we are using required annotations in
this table and we have annotated `salary` as required. 

The resulting query is

    [lang=sql]
    SELECT MAX(salary) FROM sample_08 WHERE total_emp > 1000

### __count__ *)
let query5 = hiveQuery {for line in context.sample_07 do
                        where (line.salary ?*? line.total_emp ?> 1000000)
                        count}
(** In the example above, we count the number of occupations in which people collectively earn more
than 1 million, using nullable operators. 

The resulting query is

    [lang=sql]
    SELECT COUNT(*) FROM sample_07 WHERE salary * total_emp > 1000000

### __distinct__ *)
let query6 = hiveQuery {for line in context.sample_08 do
                        where (line.total_emp ?< 500)
                        select line.description
                        distinct}

query6.Run()

(**
The resulting query is

    [lang=sql]
    SELECT DISTINCT(description) FROM sample_08 WHERE total_emp < 500

### __take__ 
Limit the number of results returned by the query. *)
let query7 = hiveQuery {for line in context.sample_08 do
                        where (line.salary > 40000)
                        select line.description
                        take 10}


(** 
The resulting query is

    [lang=sql]
    SELECT description FROM sample_08 WHERE salary > 40000 LIMIT 10

### __timeout__
Specifies a timeout (in seconds) for the query. An exception is thrown if the query times out.
*)
let query9 = hiveQuery {for line in context.sample_07 do
                        timeout 2
                        count}

(**
### __Sampling bucketed tables__

The sample tables from the Hortonworks Sandbox are not partitioned or bucketed in any way. We will
first create a copy of `sample_07` in which the data is split into 5 buckets.
 *)
context.DataContext.ExecuteCommand("set hive.enforce.bucketing = true")
context.DataContext.ExecuteCommand("CREATE TABLE sample_bucket (code STRING, description STRING, total_emp INT, salary INT)
                                    CLUSTERED BY (code) SORTED BY (salary) INTO 5 BUCKETS")
context.DataContext.ExecuteCommand("INSERT INTO TABLE sample_bucket SELECT * FROM sample_07")

(** _Note:_ You might need to close and re-open the IDE for the newly created table to be provided.
We can now sample the data from a specific bucket (e.g. from bucket 2 out of 5) using 
`sampleBucket bucket_number number_of_buckets` *)
let query10 = hiveQuery {for line in context.sample_bucket do
                         select line.description
                         sampleBucket 2 5}

(**
The resulting query is

    [lang=sql]
    SELECT * FROM sample_bucket TABLESAMPLE(BUCKET 2 OUT OF 5)

### __Write selections to a distributed file__ 
_Note:_ Although the translation of this query will work when executed directly in Hive, some issues with user privileges
prevent it from running from the F# script. *)
let query11 = hiveQuery { for line in context.sample_bucket do
                          writeDistributedFile "file1" (line.salary) 
                          where (line.description.Length < 15)
                          writeDistributedFile "file2" (line.description) 
                          select line.code
                          }

(**
### __Create new table from selection__
*)
let high_income = hiveQuery { for line in context.sample_07 do
                              where (line.salary ?> 30000)
                              newTable "high_income" line}

(**
After this, you should close and re-open your IDE in order to access the new table.

### __Overwrite table with selection__
*)
let query12 = hiveQuery {for line in context.high_income do 
                         where (line.salary ?> 40000)
                         writeRows (context.high_income.NewRow(line.code, line.description, line.total_emp, line.salary)) }

(**
The query gets translated into

    [lang=sql]
    INSERT OVERWRITE TABLE high_income SELECT code,description,total_emp,salary FROM high_income WHERE salary > 40000
    SELECT * FROM high_income WHERE salary > 40000

### __Insert rows in a table from selection__
*)
let query13 = hiveQuery {for line in context.sample_07 do
                         where (line.salary ?< 20000)
                         insertRows (context.high_income.NewRow(line.code, line.description, line.total_emp, line.salary))}
(** 
The query gets translated into

    [lang=sql]
    INSERT INTO TABLE high_income SELECT code,description,total_emp,salary FROM sample_07 WHERE salary < 20000
    SELECT * FROM sample_07 WHERE salary < 20000

**[Bug]** Accessing tables by name and manually applying a schema. 
------------------------------------
_Since `HiveTable` no longer implements `IQueryable`, they cannot be used in F# query expressions
and they are not yet supported in `hiveQuery` expressions._

This approach doesn't take advantage of the type checking that type providers enable.
Table names and schemas are imported as strings. We need to construct the type for the 
schema ourselves. Here, we are declaring the schema as a tuple of primitive types.
Also, whenever a query is made on a partially typed table, all the data is retrieved and then the
query gets executed on the client side.
*)
let timeout = 5<s>
let hive = Hive.HiveRuntime.HiveDataContext(dsn, 1000*timeout, 1000*timeout)
let (table : HiveTable<(string * string * int * int)>) = hive.GetTable("sample_07")

(** The query below retrieves an entry that contains a null.
Note that null values will be replaced by their default type value (0 in the case of `int` values). *)
(*** define-output:query0 ***)
let query' = hiveQuery { for row in table do
                         where ((fun (_, description, _, _) -> description = "Actors") row)
                         select row}
query'.Run()

(** The result of this query will be: *)

(** where the null has been replaced by a 0. If this is not the desired behaviour, 
it can be avoided by using `System.Nullable<int>` instead of the F# `int` type when
constructing the table schema. *)
type NullableSchema = string * string * System.Nullable<int> * System.Nullable<int>
let (table' : HiveTable<NullableSchema>) = hive.GetTable("sample_07")

(** We can now run the same query as in our first, strongly typed example, again using
nullable operators. *)
hiveQuery { for row in table' do
            where ((fun (_, _, _, salary) -> salary ?< 20000) row)
            select row}



(**
Writing lambdas in the `where` clause of the query in order to handle tuples can get quite 
cumbersome. An easier way to handle partially typed tables is using records instead. Everything
else remains unchanged in the example below.
*)
type SchemaRecord =
    {Code : string;
     Description : string;
     Total_Emp : System.Nullable<int>;
     Salary : System.Nullable<int>;}

let (table2 : HiveTable<SchemaRecord>) = hive.GetTable("sample_07")
hiveQuery { for row in table2 do
            where (row.Salary ?< 20000)
            select row.Description }
