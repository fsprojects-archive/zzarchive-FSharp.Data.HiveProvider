namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSharp.Data.HiveProvider")>]
[<assembly: AssemblyProductAttribute("FSharp.Data.HiveProvider")>]
[<assembly: AssemblyDescriptionAttribute("Easily query your Hive database in F# projects")>]
[<assembly: AssemblyVersionAttribute("0.0.2")>]
[<assembly: AssemblyFileVersionAttribute("0.0.2")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.2"
