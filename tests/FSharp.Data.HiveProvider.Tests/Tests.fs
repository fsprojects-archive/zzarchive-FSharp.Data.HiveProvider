module FSharp.Data.HiveProvider.Tests

open Hive.HiveRuntime
open NUnit.Framework
open System.Data.Odbc
open Microsoft.FSharp.Linq.NullableOperators

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

[<Literal>]
let dsn = "Sample Hortonworks Hive DSN; pwd=hadoop"
type Conn = Hive.HiveTypeProvider<dsn, DefaultMetadataTimeout=1000>
let context = Conn.GetDataContext()

(** This allows us to see the raw requests sent to Hadoop. *)
context.DataContext.RequestSent.Add(fun msg -> printf "REQUEST: %s" msg)


[<Test>]
let ``Select should work when selecting multiple columns`` () = 

    let query6 = hiveQuery {for line in context.sample_08 do
                            where (line.total_emp ?< 500)
                            select (line.description, line.salary) }

    let queryText = query6.GetQueryString()

    Assert.AreEqual("SELECT description, salary FROM sample_08 WHERE (total_emp) < (500)", queryText)

[<Test>]
let ``Distinct should work when selecting multiple columns`` () = 

    let query6 = hiveQuery {for line in context.sample_08 do
                            where (line.total_emp ?< 500)
                            select (line.description, line.salary)
                            distinct}

    let queryText = query6.GetQueryString()

    Assert.AreEqual("SELECT DISTINCT description, salary FROM sample_08 WHERE (total_emp) < (500)", queryText)

    ()


[<Test>]
let ``Where works with &&``() = 

    let query6 = hiveQuery {for line in context.sample_08 do
                            where ((line.total_emp ?< 500) &&  (line.salary > 100))
                            select (line.description, line.salary)
                            distinct}

    let queryText = query6.GetQueryString()

    Assert.AreEqual("SELECT DISTINCT description, salary FROM sample_08 WHERE ((total_emp) < (500)) AND ((salary) > (100))", queryText)

    ()


[<Test>]
let ``Where works with || and not``() = 

    let query6 = hiveQuery {for line in context.sample_08 do
                            where ((line.total_emp ?< 500) || not  (line.salary > 100))
                            select (line.description, line.salary)
                            distinct}

    let queryText = query6.GetQueryString()

    Assert.AreEqual("SELECT DISTINCT description, salary FROM sample_08 WHERE ((total_emp) < (500)) OR (NOT((salary) > (100)))", queryText)

    ()
