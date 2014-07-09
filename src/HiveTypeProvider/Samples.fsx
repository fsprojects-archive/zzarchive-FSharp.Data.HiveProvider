#r @"../../bin/HiveTypeProvider.dll"
open Hive.HiveRuntime
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols

//-------------------------------------------------------
// Statically typed view of existing tables
[<Literal>]
let dsn = "Sample Hortonworks Hive DSN; pwd=hadoop"
type Conn = Hive.HiveTypeProvider<dsn>

let ctxt = Conn.GetDataContext()

query {for line in ctxt.sample_07 do
       where (line.salary.Value > 20000)
       select line.description}

//--------------------------------------------------------
// Partially typed view of tables using the underlying runtime support
//
// - All column names and table names are strings

let timeout = 5<s>
let hive = Hive.HiveRuntime.HiveDataContext(dsn, 1000*timeout, 1000*timeout, true)

//Manually declaring table schema types
let (table : HiveTable<(string * string * int * int)>) = hive.GetTable("sample_07")
query { for line in table do
        select line}
      |> Seq.filter (fun (_, _, _, salary) -> salary > 20000)
      |> Seq.map (fun (_, description, _, _) -> description)
      |> Array.ofSeq

//Manually construct a record type as the table schema
type Line =
    {Code : string;
     Description : string;
     Total_Emp : int;
     Salary : int;}

let (table2 : HiveTable<Line>) = hive.GetTable("sample_07")
query {for line in table2 do
       where (line.Salary > 20000)
       select line.Description}
       |> Array.ofSeq

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