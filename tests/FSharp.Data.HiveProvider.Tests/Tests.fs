module FSharp.Data.HiveProvider.Tests

open HiveTypeProvider
open NUnit.Framework
open System.Data.Odbc

[<Test>]
let ``hello returns 42`` () =
  let result = 42
  printfn "%i" result
  Assert.AreEqual(42,result)

//Just a toy test. Will prevent building connection to Hadoop VM can't be made.
[<Test>]
let ``Connection Test`` () = 
    let dsn = "Sample Hortonworks Hive DSN; PWD=hdoop" 
    let timeout = 5
    let conn = new OdbcConnection (sprintf "DSN=%s" dsn,ConnectionTimeout=int timeout*1000)
    conn.Open()
