#r @"../../bin/HiveTypeProvider.dll"
open Hive
open HiveTypeProvider
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols

let dsn = "Sample Hortonworks Hive DSN; pwd=hadoop"
let timeout = 5<s>

let hive = HiveTypeProvider.HiveDataContext(dsn, 1000*timeout, 1000*timeout, true)

//Create and delete a table
hive.ExecuteCommand("CREATE TABLE mock (id INT, name STRING, age INT)")
hive.ExecuteCommand("DROP TABLE mock")

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


//let table = hive.GetTable("sample_07")
//let query = HiveQueryBuilder()
//query.Count(table)
