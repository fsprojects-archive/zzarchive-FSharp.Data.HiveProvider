#r @"../../bin/HiveTypeProvider.dll"
open Hive.HiveRuntime
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols
open Microsoft.FSharp.Linq.NullableOperators

//-------------------------------------------------------
// Statically typed view of existing tables
[<Literal>]
let dsn = "Sample Hortonworks Hive DSN; pwd=hadoop"
type Conn = Hive.HiveTypeProvider<dsn,DefaultMetadataTimeout=1000>

let context = Conn.GetDataContext()
context.DataContext.RequestSent.Add (fun data -> printfn "REQUEST: %s" data)

let query = hiveQuery {for line in context.sample_07 do
                       where (line.salary ?< 20000)
                       select line.description}
query.Run(timeout=1000<s>)

let query2 = hiveQuery {for line in context.sample_07 do
                        where (line.salary ?< 30000)
                        averageBy (float (line.salary.Value))}

//--------------------------------------------------------
// Partially typed view of tables using the underlying runtime support
//
// - All column names and table names are strings

let timeout = 5<s>
let hive = Hive.HiveRuntime.HiveDataContext(dsn, 1000*timeout, 1000*timeout, true)

type NullableSchema = string * string * System.Nullable<int> * System.Nullable<int>
let (table : HiveTable<NullableSchema>) = hive.GetTable("sample_07")

hiveQuery { for row in table do
            where ((fun (_, _, _, salary) -> salary ?< 20000) row)
            select row}
          |> printfn "%A"

//Manually construct a record type as the table schema
type SchemaRecord =
    {Code : string;
     Description : string;
     Total_Emp : System.Nullable<int>;
     Salary : System.Nullable<int>;}

let (table2 : HiveTable<SchemaRecord>) = hive.GetTable("sample_07")
hiveQuery { for row in table2 do
            where (row.Salary ?< 20000)
            select row.Description }
          |> printfn "%A"

//Print the schema of a random table from the system
let randomSchema =
    let (|Len|) a = Array.length a
    let names = hive.GetTableNames()
    match names with
    | Len 0 -> printfn "No tables found"
    | Len n -> let random = new System.Random()
               names.[random.Next(Array.length names)]
               |> hive.GetTableSchema
               |> printfn "%A"

//Create and delete a table
hive.ExecuteCommand("CREATE TABLE mock (id INT, name STRING, age INT)")
hive.ExecuteCommand("DROP TABLE mock")


//Using required annotations
type Conn' = Hive.HiveTypeProvider<dsn,UseRequiredAnnotations=true>
let context' = Conn'.GetDataContext()
context'.DataContext.RequestSent.Add (fun data -> printfn "REQUEST: %s" data)


let query' = hiveQuery {for line in context'.new_sample do
                        where (line.salary ?< 20000)
                        select line.description}

query'.Run()


//hive.ExecuteCommand("ALTER TABLE sample_08 CHANGE salary salary int COMMENT '(required)'")

let query2 = hiveQuery {for line in context'.sample_08 do
                        where (line.description = "Actors")
                        select line.salary}

query2.Run()


//Other stuff:

context.DataContext.ExecuteCommand("DROP TABLE sample_bucket")


let newTable = hiveQuery {for line in context.sample_bucket do
                          newTable "table4" line.description }

newTable.GetSchema()
newTable.TableName
newTable.GetPartitionNames()

let queryYY = hiveQuery { for line in context.sample_bucket do
                          writeDistributedFile "file5" (line.description, line.salary) 
                          distinct
                          writeDistributedFile "file6" (line.description, line.salary) 
                          select line.code
                          }


hiveQuery {for line in context.sample_07 do
           where (line.description.Length > 10)
           count}


hiveQuery {for line in context.sample_07 do
           where (line.description.Replace("a","b") = "bbb" )
           count}

hiveQuery {for line in context.sample_07 do
           where (line.description.Replace("a","b") = "bbb" )
           count}

hiveQuery {for line in context.sample_07 do
           where (line.total_emp.Value = 1)
           count}



hiveQuery {for line in context.sample_07 do
           where (double line.total_emp.Value * 2.0 = 2.0)
           select line }
   |> string


hiveQuery {for line in context.sample_07 do
           where (line.total_emp.Value * 2 = 2)
           select line }
   |> string




hiveQuery.NewTable (hiveQuery.For(context.sample_bucket, 
                                  (fun line -> hiveQuery.Yield line)), 
                    "table4",
                    (fun line -> line.description))



