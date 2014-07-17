module Hive.HiveRuntime

open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open System
open System.Text
open System.Linq
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open System.IO
open Microsoft.FSharp.Control.WebExtensions
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols
open System.Threading
open System.Collections.Generic
open System.Diagnostics
open System.ComponentModel 
open System.IO.Pipes
open System.Text.RegularExpressions
open System.Reflection
open System.Data.Odbc
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols
open FSharp.ProvidedTypes


[<AutoOpen>]
module (*internal*) HiveSchema =

    let inline (|Value|Null|) (x:System.Nullable<_>) = if x.HasValue then Value x.Value else Null 
    let inline Value x = System.Nullable x
    let inline Null<'T when 'T : (new : unit -> 'T) and 'T : struct and 'T :> System.ValueType> = System.Nullable<'T>()

    /// Represents a type of a column in a hive table
    type DBasic =
        | DInt8
        | DInt16
        | DInt32 
        | DInt64
        | DSingle 
        | DDouble 
        | DDecimal 
        | DBoolean 
        | DAtom 
        | DMap of DBasic * DBasic
        | DArray of DBasic // note, accessing these columns NYI
        | DStruct of (string * DBasic) [] // note, accessing these columns NYI
        | DTable of HiveColumnSchema[] // result of 'select *'
        | DString 

    and HiveColumnSchema = 
        { HiveName: string
          Description: string
          HiveType: DBasic
          IsRequired: bool }

    and internal HiveTableSortKey = 
        { Column: string; Order: string }

    and HiveTableSchema = 
        { Description: string
          Columns: HiveColumnSchema[]
          PartitionKeys: string[]
          //BucketKeys: string[]
          //SortKeys: HiveTableSortKey[] 
        }


    let rec formatHiveType ty = 
        match ty with
        | DSingle _ -> "float" 
        | DDouble _ -> "double" 
        | DDecimal _ -> "decimal" 
        | DString -> "string" 
        | DInt32 -> "int" 
        | DInt8 -> "tinyint" 
        | DInt16 -> "smallint" 
        | DInt64 -> "bigint" 
        | DBoolean -> "bool" 
        | DMap(ty1,ty2) -> "map<" + formatHiveType ty1 + "," + formatHiveType ty2 + ">"
        | DArray(ty1) -> "array<" + formatHiveType ty1 + ">"
        | DStruct _cols -> failwith "struct types: nyi"
        | _ -> failwith (sprintf "unexpected type '%A'" ty)

    let rec formatHiveComment (req) = 
        match req with
        | true -> "(required)"
        | false -> ""

    /// Represents a value in one column of one row in a hive table
    type TVal =
        | VInt8 of System.Nullable<sbyte>
        | VInt16 of System.Nullable<int16>
        | VInt32 of System.Nullable<int32>
        | VInt64 of System.Nullable<int64>
        | VSingle of System.Nullable<single>
        | VDecimal of System.Nullable<decimal>
        | VDouble of System.Nullable<double>
        | VBoolean of System.Nullable<bool>
        | VString of string  // may be null
        | VMap of (TVal * TVal) [] // may be null
        | VArray of TVal[] // may be null
        | VStruct of (string * TVal)[] // may be null

    /// Represents an expression in a hive 'where' expression
    type TExpr =
        | EVal of TVal * DBasic
        | ETable of string * HiveColumnSchema[]
        | EColumn of string * DBasic
        | EQuery of HiveQueryData // e.g. 'AVG(height)'
        | EBinOp of TExpr * string * TExpr * DBasic
        | EFunc of string * TExpr list * DBasic

    /// Represents a hive query
    and HiveQueryData = 
        | Table of string * HiveColumnSchema[] 
        | GroupBy of HiveQueryData  * TExpr // Not fully implemented - see comments for #if GROUP_BY below
        | Distinct of HiveQueryData 
        | Where of HiveQueryData * TExpr 
        | Limit of HiveQueryData * int
        | Count of HiveQueryData 

        /// A 'sumBy' or other aggregation operation.
        /// The 'bool' indicates whether the result would get labelled as 'required' if we write the table.
        | AggregateOpBy of string * HiveQueryData * TExpr * bool

        | TableSample of HiveQueryData * int * int

        /// A 'select' operation.
        /// The 'bool' indicates whether the field would get labelled as 'required' if we write the table.
        | Select of HiveQueryData * (string * TExpr * bool)[] 

    let rec typeOfExpr e = 
        match e with 
        | EVal (_,ty) -> ty
        | ETable (_,ty) -> DTable ty
        | EColumn (_,ty) -> ty
        | EQuery q -> 
            match q with 
            | AggregateOpBy(_,_,e,_) -> typeOfExpr e 
            | Count(_) -> DInt64
            | q -> failwith (sprintf "unsupported query embedded in expression: %A" q)
        | EBinOp (_,_,_,ty) -> ty
        | EFunc (_,_,ty) -> ty
    
    type HiveRequest =
        | GetTableNames
        | GetTablePartitionNames of string
        | GetTableDescription of string 
        | GetTableSchema of string 
        | GetDataFrame of HiveQueryData * HiveColumnSchema[]
        | ExecuteCommand of string
  
    type HiveResult =
        | TableNames of string[]
        // tableDescription, first 10 rows
        | TableDescription of string*string
        | TablePartitionNames of string[]
        | TableSchema of HiveTableSchema
        // data[][]
        | DataFrame of TVal[][]
        | Exception of string
        | CommandResult of int

    let rec internal formatExpr expr =
        match expr with 
        | ETable (_v,_ty) -> "*"
        | EVal (v,ty) -> 
            match v with 
            | VInt8 (Value n) -> string n
            | VInt16 (Value n) -> string n
            | VInt32 (Value n) -> string n
            | VInt64 (Value n)  -> string n
            | VSingle (Value n) -> string n 
            | VDouble (Value n) -> string n 
            | VDecimal (Value n) -> string n 
            | VBoolean (Value b) -> string b
            | VString null -> "null"
            | VString s -> 
                // "String literals can be expressed with either single quotes (') or double quotes ("). Hive uses C-style escaping within the strings."
                //  (from https://cwiki.apache.org/confluence/display/Hive/Literals)
                sprintf "'%s'" (s.Replace("'", @"\'"))
            | VInt8 Null -> "null"
            | VInt16 Null -> "null"
            | VInt32 Null -> "null"
            | VInt64 Null -> "null"
            | VSingle Null -> "null"
            | VDouble Null -> "null"
            | VDecimal Null -> "null"
            | VBoolean Null -> "null"
            | VMap _ | VArray _ | VStruct _ -> "<nyi: map/array/struct>"
        | EColumn (nm,ty) -> nm
        | EBinOp(arg1,op,arg2,_) -> "(" + formatExpr arg1 + ")" + " " + op  + " " + "(" + formatExpr arg2 + ")"
        | EQuery(q) -> 
            if formatQueryAux q <> "" then failwith (sprintf "unsupported nested query: %A" q)
            formatQuerySelect q
        | EFunc("$id",[e],_) -> formatExpr e // used for changing types
        | EFunc(op,[],_) -> op 
        | EFunc(op,args,_) -> op + "(" + (List.map formatExpr args |> String.concat ",") + ")"


    and getTableName q = 
        match q with 
        | Table(tableName,_) -> tableName
        | Distinct q2 | GroupBy(q2,_) | AggregateOpBy(_,q2,_,_) | Select(q2,_) | Count(q2) | Limit(q2, _) | Where(q2,_) | TableSample(q2,_,_) -> getTableName q2

    // https://cwiki.apache.org/Hive/languagemanual.html
    and internal formatQuerySelect q = 
        match q with 
        | Table(_tableName, _) -> "*"
        | Select (_q2,colExprs) -> colExprs |> Array.map (fun (_,e,_) -> formatExpr e) |> String.concat ", "
        | AggregateOpBy (op, _q2,selector,_) -> op + "(" + formatExpr selector + ")"
        | TableSample (q2,_n1,_n2) -> formatQuerySelect q2 
        | GroupBy (q2,_) -> formatQuerySelect q2 
        | Limit (q2,_rowCount) -> formatQuerySelect q2
        | Count (q2) -> "COUNT(" + formatQuerySelect q2 + ")"
        | Distinct (q2) -> "DISTINCT " + formatQuerySelect q2
        | Where (q2,_pred) -> formatQuerySelect q2 

    and internal formatQueryAux q = 
        match q with 
        | Table(_tableName,_) -> ""
        | Select (q2,_colExprs) -> formatQueryAux q2
        | GroupBy (q2,keyExpr) -> formatQueryAux q2 + " GROUP BY " + formatExpr keyExpr
        | AggregateOpBy (_op, q2,_selector,_) -> formatQueryAux q2
        | TableSample (q2,n1,n2) -> formatQueryAux q2 + sprintf " TABLESAMPLE(BUCKET %d OUT OF %d)" n1 n2
        | Limit (q2,rowCount) -> formatQueryAux q2 + sprintf " LIMIT %i" rowCount
        | Count (q2) -> formatQueryAux q2
        | Distinct (q2) -> formatQueryAux q2
        | Where (q2,pred) -> formatQueryAux q2 + " WHERE " + formatExpr pred

    let internal formatQuery q =  
        "SELECT " + formatQuerySelect q + " FROM " + getTableName q + formatQueryAux q


type internal IHiveDataServer = 
    inherit System.IDisposable
    abstract GetTableNames : unit -> string[]
    abstract GetTablePartitionNames : tableName: string -> string[]
    abstract GetTableSchema : tableName: string -> HiveTableSchema
    abstract ExecuteCommand : string -> int
    abstract GetDataFrame : HiveQueryData * HiveColumnSchema[] -> TVal[][] 

#nowarn "40" // recursive objects

[<AutoOpen>]
module internal DetailedTableDescriptionParser = 
    open System
    open System.Collections.Generic
    type Parser<'T> = P of (list<char> -> ('T * list<char>) option)

    let result v = P(fun c -> Some (v, c) )
    let zero () = P(fun _c -> None)
    let bind (P p) f = P(fun inp ->
      match p inp with 
      | Some (pr, inp') -> 
               let (P pars) = f pr
               pars inp' 
      | None -> None)
    let plus (P p) (P q) = P (fun inp -> match p inp with Some x -> Some x | None -> q inp )

    let (<|>) p1 p2 = plus p1 p2

    type ParserBuilder() =
      member x.Bind(v, f) = bind v f
      member x.Zero() = zero()
      member x.Return(v) = result(v)
      member x.ReturnFrom(p) = p
      member x.Combine(a, b) = plus a b
      member x.Delay(f) = f()

    let parser = new ParserBuilder()

    /// Accept any character
    let anyChar = P(function | [] -> None | c :: r -> Some (c,r))
    /// Accept any character satisfyin the predicate
    let sat p = parser { let! v = anyChar in if (p v) then return v }
    /// Accept precisely the given character
    let char x = sat ((=) x)
    /// Accept any whitespace character
    let whitespace = sat (Char.IsWhiteSpace)

    /// Accept zero or more repetitions of the given parser
    let rec many p = 
      parser { let! it = p in let! res = many p in return it::res }  
      <|> 
      parser { return [] }

    /// Accept zero or one repetitions of the given parser
    let optional p = 
      parser { let! v = p in return Some v }
      <|> 
      parser { return None }             

    /// Run the given parser
    let apply (P p) (str:seq<char>) = 
      let res = str |> List.ofSeq |> p
      match res with 
      | Some (x,[]) -> Some x
      | _ -> None

    /// Accept one or more repetitions of the given parser with the given character separator followed by whitespace
    let oneOrMoreSep f sep =
        parser { let! first = f
                 let! rest = many (parser { let! _ = char sep in let! _ = many whitespace in let! w = f in return w })
                 return (first :: rest) }

    /// Accept zero or more repetitions of the given parser with the given character separator followed by whitespace
    let zeroOrMoreSep f sep =
        oneOrMoreSep f sep <|> parser { return [] }

    /// Results of parsing the detailed table description
    [<RequireQualifiedAccess>]
    type Data = 
        | Node of string * IDictionary<string,Data>
        | List of Data list
        | Value of string 

    let anyNonDelim lhs = 
        if lhs then sat (fun c -> c <> '=' && c <> '{' && c <> '(' && c <> ')' && c <> ',' && c <> ':' && c <> '[' && c <> ']' && c <> '}' && not (Char.IsWhiteSpace c))
        else sat (fun c -> c <> '(' && c <> '{' && c <> '[' && c <> ')' && c <> ']' && c <> '}' && c <> ',' && not (Char.IsWhiteSpace c))

    let word lhs =
        parser { let! ch = anyNonDelim lhs
                 let! chs = many (anyNonDelim lhs)
                 return String(Array.ofSeq (ch::chs)) } 

    /// Parse either
    //      1023
    //      Foo
    //      Foo(a1:b1,...,an:bn)
    //      [a1,...,an]
    let rec data lhs = 
        parser { let! w = word lhs 
                 let! rest = optional (parser { let! _ = char '('
                                                let! rest = oneOrMoreSep dataField ',' 
                                                let! _ = char ')'
                                                return rest })
                 return (match rest with None -> Data.Value w | Some xs -> Data.Node(w,dict xs)) }
        <|>
        parser { let! xs = dataList
                 return Data.List(xs) }
        <|>
        record
    /// Parse left of 'a:int' or 'a=3' labelled data
    and lhsData = data true
    /// Parse right of 'a:int' or 'a=3' labelled data
    and rhsData = data false
    /// Parse a a1:b1 data field, e.g. in FieldSchema(a:int)
    and dataField = 
        parser { let! w = word(true)
                 let! _ = char ':'
                 let! d = if w = "comment" then commentData elif w = "type" then typeData else rhsData
                 return (w,d) }
    /// Parse a {a1=b1,...,an=bn} record
    and record = 
        parser { let! _ = char '{'
                 let! rest = zeroOrMoreSep recordField ',' 
                 let! _ = char '}'
                 return Data.Node("Record",dict rest) }

    /// Parse a {...a=b...} field
    and recordField = 
        parser { let! w = word(true)
                 let! _ = char '='
                 let! d = if w = "comment" then commentData elif w = "type" then typeData else rhsData
                 return (w,d) }

    /// Parse a comment, which can include balanced parens
    and commentData = delimData  ('(', ')', '{', '}', (fun c -> c <> ')' && c <> '}'))

    /// Parse a type, e.g. 'map<string,string>'
    and typeData = delimData ('<','>', '<', '>', (fun c -> c <> ')' && c <> '}' && c <> ','))

    /// Parse free text where 'startParen' and 'endParen' are balanced and we terminate when 'delim' is false
    and delimData (startParen1,endParen1,startParen2,endParen2,delim) = 
        parser { let! chs = delimText (startParen1,endParen1,startParen2,endParen2,delim) 0
                 return Data.Value(String(Array.ofSeq chs)) } 

    /// Parse free text where 'startParen' and 'endParen' are balanced and we terminate when 'delim' is false
    and delimText (startParen1,endParen1,startParen2,endParen2,endText) parenCount = 
        parser { let! w = (char startParen1 <|> char startParen2)
                 let! d = delimText (startParen1,endParen1,startParen2,endParen2,endText) (parenCount+1)
                 return (w::d) }
        <|>
        parser { let! c = sat (fun c -> parenCount > 0 && (c = endParen1 || c = endParen2))
                 let! d = delimText (startParen1,endParen1,startParen2,endParen2,endText) (parenCount-1)
                 return (c::d) }
        <|>
        parser { let! c = sat (fun c -> parenCount > 0 || (endText c))
                 let! d = delimText (startParen1,endParen1,startParen2,endParen2,endText) parenCount
                 return (c::d) }
        <|>
        parser { return [] }

    /// Parse [FieldSchema(...),...,FieldSchema(...)]
    and dataList = 
        parser { let! _ = char '['
                 let! rest = zeroOrMoreSep lhsData ',' 
                 let! _ = char ']'
                 return rest }

    type IDictionary<'Key,'Value> with 
        member x.TryGet(key) = if x.ContainsKey key then Some x.[key] else None


    let (|ConstructedType|_|) (s:string) = 
        match s.Split('<') with 
        | [| nm; rest |] -> 
            match rest.Split('>') with 
            | [| args; "" |] -> 
                match args.Split( [| ',' |], System.StringSplitOptions.RemoveEmptyEntries) with 
                | [| |] -> None
                | args -> Some (nm,List.ofArray args)
            | _ -> None
        | [| nm |] -> Some(nm,[ ])
        | _ -> None
    //(|ConstructedType|_|) "map<int,int>" = Some ("map", [ "int"; "int" ])
    //(|ConstructedType|_|) "array<int>"= Some ("array", [ "int" ])
    //(|ConstructedType|_|) "array<>"= None
    //(|ConstructedType|_|) "array"= Some("array", [])
    
    let rec parseHiveType (s:string) = 
        match s with
        | "float" -> DSingle 
        | "double" -> DDouble 
        | "decimal" -> DDecimal 
        | "string" -> DString
        | "int" -> DInt32 
        | "tinyint" -> DInt8
        | "smallint" -> DInt16
        | "bigint" -> DInt64
        | "bool" -> DBoolean
        | ConstructedType("map", [arg1; arg2]) -> DMap(parseHiveType arg1, parseHiveType arg2)
        | ConstructedType("array", [arg1]) -> DArray(parseHiveType arg1)
        | _ -> failwith (sprintf "unknown type '%s'" s)

    // Extract if the field is required (not nullable)
    let parseHiveColumnAnnotation (desc:string) = (desc.Trim() = "(required)")

    type FieldSchema = { Name: string; Type:DBasic; Comment: string }
    type DetailedTableInfo = 
        { PartitionKeys: FieldSchema[]
          //BucketKeys: string[]
          //SortKeys: HiveTableSortKey[]
          Columns: FieldSchema[]
          Comment: string }

(*
    /// Get the names of the bucket columns out of the parsed detailed table description
    //bucketCols:[userid]
    let getBucketColumns r = 
       [| match r with 
          | Data.Node("Table",d) ->
              d.ToList |> printfn "%A"
              match d.TryGet "bucketCols" with 
              | Some (Data.List d2) -> 
                  for v in d2 do 
                      match v with 
                      | Data.Value(colName) -> yield colName
                      | _ -> ()
              | _ ->  ()
          | _ ->  () |]

    /// Get the names of the sort columns out of the parsed detailed table description
    //sortCols:[Order(col:viewtime, order:1)]
    let getSortColumns r = 
       [| match r with 
          | Data.Node("Table",d) ->
              match d.TryGet "sortCols" with 
              | Some (Data.List d2) -> 
                  for v in d2 do 
                      match v with 
                      | Data.Node("Order",d4) -> 
                            match (d4.TryGet "col", defaultArg (d4.TryGet "order") (Data.Value "1")) with 
                            | Some (Data.Value col), Data.Value order -> 
                              yield {Column=col; Order=order}
                            | _ -> ()
                      | _ ->  ()
              | _ ->  ()
          | _ ->  () |]

*)

    /// Get the partition keys out of the parsed detailed table description
    let getPartitionKeys r = 
       [| match r with 
          | Data.Node("Table",d) ->
              match d.TryGet "partitionKeys" with 
              | Some (Data.List d2) -> 
                  for v in d2 do 
                      match v with 
                      | Data.Node("FieldSchema",d4) -> 
                            match (d4.TryGet "name", d4.TryGet "type", defaultArg (d4.TryGet "comment") (Data.Value "")) with 
                            | Some (Data.Value nm), Some (Data.Value typ), Data.Value comment -> 
                              yield {Name=nm; Type=parseHiveType typ; Comment=comment}
                            | _ -> ()
                      | _ ->  ()
              | _ ->  ()
          | _ ->  () |]

    /// Get the getFields out of the parsed detailed table description
    let getFields r = 
       [| match r with 
          | Data.Node("Table",d) ->
              match d.["sd"] with 
              | Data.Node("StorageDescriptor",d2) ->
                 match d2.TryGet "cols" with 
                 | Some (Data.List cols) ->
                   for col in cols do 
                       match col with 
                       | Data.Node("FieldSchema",d4) -> 
                           match (d4.TryGet "name", d4.TryGet "type", defaultArg (d4.TryGet "comment") (Data.Value "")) with 
                           | Some (Data.Value nm), Some (Data.Value typ), Data.Value comment -> 
                              yield {Name=nm; Type=parseHiveType typ; Comment=comment}
                           | _ -> ()
                       | _ ->  ()
                 | _ ->  ()
              | _ ->  ()
          | _ ->  () |]

    /// Get the 'comment' field out of the parsed detailed table description
    let getComment r = 
        match r with 
        | Data.Node("Table",d) ->
            match d.TryGet "parameters" with 
            | Some (Data.Node(_,d2)) ->
                match d2.TryGet "comment" with 
                | Some (Data.Value comment) -> comment
                | _ ->  ""
            | _ ->  ""
        | _ ->  ""

    let getDetailedTableInfo (text:string) = 
        apply lhsData text 
        |> Option.map (fun r -> 
            { PartitionKeys=getPartitionKeys r; 
              Columns = getFields r 
              //SortKeys = getSortColumns r 
              //BucketKeys = getBucketColumns r 
              Comment=getComment r})

type internal OdbcDataServer(dsn:string,timeout:int<s>) = 
    let conn = 
            let conn = new OdbcConnection (sprintf "DSN=%s" dsn,ConnectionTimeout=int timeout*1000)
            conn.Open()
            conn

    let runReaderComand (cmd:string) = 
        System.Diagnostics.Debug.WriteLine(sprintf "executing query command \"%s\"" cmd)
        let command = conn.CreateCommand(CommandTimeout=int timeout*1000, CommandText=cmd)
        command.ExecuteReader()

    let runNonQueryComand (cmd:string) = 
        System.Diagnostics.Debug.WriteLine(sprintf "executing non-query command \"%s\"" cmd)
        let command = conn.CreateCommand(CommandTimeout=int timeout*1000, CommandText=cmd)
        command.ExecuteNonQuery()

    let GetTableSchema tableName = 
        let xs = 
            [| use reader = runReaderComand (sprintf "DESCRIBE extended %s" tableName) 
                       
               while reader.Read() && not (String.IsNullOrWhiteSpace (reader.GetString(0))) do 
                    let colName = reader.GetString(0)
                    // These are the 'simple' description of the table columns. If anything goes wrong
                    // with parsing the detailed description then we assume there are no partition
                    // keys and use the information extracted from the simple description instead.
                    let datatype = parseHiveType (reader.GetString(1).ToLower())
                    let desc = (if reader.IsDBNull(2) then "" else reader.GetString(2))
                    yield (None,Some((colName, desc, datatype)),None)

               while reader.Read() && (reader.GetString(0) <> "Detailed Table Information") do 
                   ()

               let detailedInfoText = reader.GetString(1)
               match getDetailedTableInfo detailedInfoText with 
               | Some detailedInfo -> 
                   yield (None, None, Some detailedInfo)
               | None -> 
                   // This case is if we failed to parse the detailed table information
                   let comment = System.Text.RegularExpressions.Regex.Match(detailedInfoText,"parameters:{[^}]*comment=([^}]*)}").Groups.[1].Value
                   yield (Some(comment),None,None)
                         
            |] 
        let tableDetailsOpt = xs |> Array.map (fun (_,_,c) -> c) |> Array.tryPick id
        let tableDetails = 
            match tableDetailsOpt with 
            | Some tableDetails -> tableDetails
            | None -> 
                let tableCommentBackup = xs |> Array.map (fun (a,_,_) -> a) |> Array.tryPick id
                let tableColumnsBackup = xs |> Array.map (fun (_,b,_) -> b) |> Array.choose id
                { Columns = tableColumnsBackup |> Array.map (fun (a,b,c) -> { Name=a; Type=c; Comment=b })
                  //SortKeys=[| |]
                  //BucketKeys=[| |]
                  PartitionKeys=[| |]
                  Comment=defaultArg tableCommentBackup ""}

        let columns = 
            tableDetails.Columns |> Array.map (fun column -> 
                let colRequired = parseHiveColumnAnnotation column.Comment
                { HiveName=column.Name 
                  Description=column.Comment
                  HiveType=column.Type
                  IsRequired=colRequired })

        let partitionKeys = tableDetails.PartitionKeys |> Array.map (fun x -> x.Name)
        { Columns=columns
          Description=tableDetails.Comment
          PartitionKeys=partitionKeys
          //BucketKeys=tableDetails.BucketKeys
          //SortKeys=tableDetails.SortKeys
        }

    interface IDisposable with 
        member x.Dispose() = 
            // TODO: we are getting long pauses on exit of fsc.exe and devenv.exe because the ODBC connection pool is 
            // being finalized. This is really annoying. I don't know what to do about this. Explicitly
            // closing doesn't help.

            //printfn "closing..."
            //System.Data.Odbc.OdbcConnection.ReleaseObjectPool()
            conn.Dispose()
            //printfn "%A" [ for f in conn.GetType().GetFields(System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.Instance ||| System.Reflection.BindingFlags.NonPublic) -> f.Name, f.GetValue(conn) ]
            //System.AppDomain.CurrentDomain.DomainUnload.Add(fun _ -> 
            //    printfn "exiting..."
            //    System.Environment.Exit(0))
            //printfn "closed!"

    interface IHiveDataServer with 

        member x.GetTablePartitionNames(tableName) = 
            [| use reader = runReaderComand ("SHOW PARTITIONS " + tableName ) ; 
               while reader.Read() do yield reader.GetString(0)  |]

        member x.GetTableNames() = 
            [| use reader = runReaderComand "SHOW TABLES" ; 
               while reader.Read() do yield reader.GetString(0)  |]

        member x.GetTableSchema tableName = GetTableSchema tableName

        member x.ExecuteCommand (commandText) = runNonQueryComand commandText

        member x.GetDataFrame (query, colData) =

          let queryText = formatQuery query
                    
          let table = 
              [| use reader = runReaderComand queryText 
                 while reader.Read() do 
                    let row = [|for i in 0..colData.Length-1 -> reader.GetValue(i)|]
                    yield [|
                        for i in 0..colData.Length-1 do
                          let colData = 
                              match colData.[i].HiveType with
                              | DInt8 -> VInt8(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToSByte o))
                              | DInt16 -> VInt16(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToInt16 o))
                              | DInt32 -> VInt32(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToInt32 o))
                              | DInt64 -> VInt64(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToInt64 o))
                              | DSingle _ -> VSingle(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToSingle o))
                              | DDouble _ -> VDouble(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToDouble o))
                              | DDecimal _ -> VDecimal(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToDecimal o))
                              | DBoolean -> VBoolean(match row.[i] with | :? DBNull -> Null | o -> Value(Convert.ToBoolean o))
                              | DString -> VString(match row.[i] with | :? DBNull -> null | o -> (o :?> string))
                              | DArray _ -> failwith "nyi map/array/struct"
                              | DMap _ -> failwith "nyi map/array/struct"
                              | DStruct _ -> failwith "nyi map/array/struct"
                              | DAtom -> failwith "unreachable: atom"
                              | DTable(_args) -> failwith "evaluating whole table as part of client-side expression"
                          yield colData 
                      |]
              |]

          table


type internal Adjustment =  
    | Center
    | Right
    | Left

let internal hiveHandler (dsn:string) (timeout:int<s>) (msg:HiveRequest) : HiveResult = 
    try
        async {
          try
            use dataServer = new OdbcDataServer(dsn,timeout) :> IHiveDataServer
            let result = 
                match msg with
                | ExecuteCommand(commandText) -> dataServer.ExecuteCommand(commandText) |> CommandResult
                | GetTableNames -> dataServer.GetTableNames()|> TableNames
                | GetTablePartitionNames(tableName) -> dataServer.GetTablePartitionNames(tableName)|> TablePartitionNames
                | GetTableDescription tableName ->
                        let table = dataServer.GetTableSchema tableName
                        let rows = dataServer.GetDataFrame (Limit(Table(tableName,table.Columns),10), table.Columns)
                        let maxWidth = 30
                        let tableHead  =
                            let inline nullToString x = match x with | null -> "NA" | x -> string x
                            let inline nullableToString (x:Nullable<'T>) = if x.HasValue then string x.Value else "NA"
                            let flip (xs:'a[][]) = [|for i in 0..xs.[0].Length - 1 -> [| for j in 0..xs.Length - 1 -> xs.[j].[i]|]|]
                            let resize (adjust:Adjustment) (length:int) (str:string) = 
                                if str.Length < length 
                                then 
                                    match adjust with
                                    | Center ->
                                        let diff = length - str.Length
                                        let leftPad = (String.replicate ((diff / 2) + (diff % 2)) " ")
                                        let rightPad = (String.replicate (diff / 2) " ")
                                        leftPad + str + rightPad
                                    | Right -> str.PadLeft(length)
                                    | Left -> str.PadRight(length)
                                elif str.Length = length then str
                                else str.Substring(0,Math.Max(0,length - 2)) + ".."

                            let tValToString =
                                function
                                | VInt8 x -> x |> nullableToString
                                | VInt16 x -> x |> nullableToString
                                | VInt32 x -> x |> nullableToString
                                | VInt64 x -> x |> nullableToString
                                | VSingle x -> x |> nullableToString
                                | VDouble x -> x |> nullableToString
                                | VDecimal x -> x |> nullableToString
                                | VBoolean x -> x |> nullableToString
                                | VString x -> x |> nullToString
                                | VArray x -> x |> nullToString
                                | VStruct x -> x |> nullToString
                                | VMap x -> x |> nullToString

                            // TODO - change adjustment based on hive type
                            
                            let dataFrame = [| for (col,column) in (table.Columns,rows |> flip) ||> Array.zip -> [| yield col.HiveName; yield! column |> Array.map tValToString |]|]

                            
                            [|
                                let lengths = dataFrame |> Array.map (fun xs -> min maxWidth ((xs |> Array.maxBy (fun x -> x.Length)).Length))
                                let hr = "+" + (lengths |> Array.map (fun length -> String.replicate length "-") |> String.concat "+") + "+"
                                yield hr
                                // Header
                                for i in 0..dataFrame.[0].Length - 1 do
                                    yield "|" + ([|for (column,length) in (dataFrame,lengths) ||> Array.zip -> resize (if i = 0 then Center else Left) length column.[i]|] |> String.concat "|") + "|"
                                    if i = 0 then yield hr
                                yield hr
                            |] |> String.concat "\n"

                        TableDescription(table.Description,tableHead)
                | GetTableSchema tableName ->
                    TableSchema (dataServer.GetTableSchema tableName)
                | GetDataFrame (query, colData) ->
                    DataFrame (dataServer.GetDataFrame (query, colData))
            return result                                
          with
          | ex -> return Exception("Error: " + ex.Message + "\n" (* + sprintf "Connection='%s'\n" cns *) + sprintf "Timeout='%d'\n" (int timeout) + sprintf "Message='%A'\n" msg + ex.ToString())
        } |> fun x -> Async.RunSynchronously(x,int timeout*1000)
    with
    | ex -> Exception("Error: " + ex.Message + "\n" (* + sprintf "Connection='%s'\n" cns *)  + sprintf "Message='%A'\n" msg + ex.ToString())


let internal HiveRequestInProcess (dsn:string, timeout:int<s>) (req:HiveRequest) = 
    hiveHandler dsn timeout req



[<AutoOpen>]
module internal StaticHelpers = 
    let memoize f = 
        let tab = System.Collections.Generic.Dictionary()
        fun x -> 
            if tab.ContainsKey x then tab.[x] 
            else let a = f x in tab.[x] <- a; a

    let rec computeFSharpTypeWithoutNullable(colHiveType) = 
        match colHiveType with 
        | DInt8  -> typeof<sbyte>
        | DInt16  -> typeof<int16>
        | DInt32  -> typeof<int32>
        | DInt64  -> typeof<int64>
        | DSingle _ -> typeof<single>
        | DDouble _ -> typeof<double>
        | DDecimal _ -> typeof<decimal>
        | DBoolean -> typeof<bool>
        | DString -> typeof<string>
        | DAtom -> failwith "should not need to convert atoms"
        | DArray(et) -> computeFSharpTypeWithoutNullable(et).MakeArrayType()
        | DMap(dt,rt) -> typedefof<IDictionary<_,_>>.MakeGenericType(computeFSharpTypeWithoutNullable dt, computeFSharpTypeWithoutNullable rt)
        | DStruct(_cols) -> failwith "nyi: struct"
        | DTable(_args) -> failwith "evaluating whole table as part of client-side expression"

    let rec colDataOfQuery q = 
        let mkColumn (colName:string, typ, isRequired) = 
            {HiveName=colName
             HiveType= typ
             Description="The column '" + colName + "' in a selected table"
             IsRequired=isRequired}
        match q with 
        | Table(tableName, tableColumns) -> tableName, tableColumns
        | Distinct q2 | Where (q2,_) | Limit (q2,_) | TableSample(q2,_,_) -> colDataOfQuery q2
        | Select (_q2,colExprs) -> 
            // Compute the types of the selected expressions
            let colData = colExprs |> Array.map (fun (colName,colExpr,isRequired) -> mkColumn (colName, HiveSchema.typeOfExpr colExpr, isRequired))
            "$select", colData
        | Count _ -> 
            "$count", [| mkColumn ("count", DInt64, true) |]
        | GroupBy(q2,e) -> 
            let _,groupColData = colDataOfQuery q2
            "$grouping", [| mkColumn ("$key", typeOfExpr e, true); mkColumn ("$group", DTable groupColData, true)  |]
        | AggregateOpBy(_,_,e,isRequired) -> 
            "$result", [| mkColumn ("$value", typeOfExpr e, isRequired) |]

module internal RuntimeHelpers = 
        
    let unexpected exp msg = 
        match msg with 
        | Exception(ex)  -> failwith ex
        | _ -> failwithf "unexpected message, expected %s, got %+A" exp msg

/// Represents one row retrieved from the Hive data store
type HiveDataRow internal (dict : IDictionary<string,obj>) = 
    member __.GetValue<'T> (colName:string) = 
        dict.[colName] :?> 'T

    member __.Item with get (colName : string) = dict.[colName]
    new (_tableName, values : (string * obj * bool) []) = new HiveDataRow (dict [| for (key,obj,_) in values -> (key,obj) |])

    static member internal GetValueMethodInfo = 
        match <@@ fun x -> (Unchecked.defaultof<HiveDataRow>).GetValue(x) @@> with 
        | Lambdas(_,Call(_,mi,_)) -> mi.GetGenericMethodDefinition() 
        | _ -> failwith "unexpected"

    static member internal HiveDataRowCtor = 
        match <@@ fun (x: (string * obj * bool)[]) -> HiveDataRow("",x) @@> with 
        | Lambdas(_,NewObject(ci,_)) -> ci
        | _ -> failwith "unexpected"
        
     override x.ToString() = "{" + ([ for KeyValue(k,v) in dict -> k+"="+string v ] |> String.concat "; ") + "}"


/// The base type for a connection to the Hive data store.
type HiveDataContext (dsn,defaultQueryTimeout,defaultMetadataTimeout) = 

    let event = Event<_>()

    let executeRequest (requestData :  HiveRequest, timeout) =

        let evData = 
            match requestData with
            | HiveRequest.GetDataFrame(queryData,_) -> sprintf "query '%s'" (formatQuery queryData)
            | HiveRequest.ExecuteCommand (cmd) -> sprintf "command %A" cmd
            | HiveRequest.GetTableDescription tableName -> sprintf "get table description %s" tableName
            | HiveRequest.GetTableNames -> sprintf "get table names"
            | HiveRequest.GetTablePartitionNames table -> sprintf "get table partition names for %s" table
            | HiveRequest.GetTableSchema table -> sprintf "get table schema for %s" table
        event.Trigger evData

        HiveRequestInProcess(dsn,timeout) requestData

    let executeQueryRequest (requestData, queryTimeout) = executeRequest (requestData, defaultArg queryTimeout defaultQueryTimeout)
    let executeMetadataRequest (requestData, metadataTimeout) = executeRequest (requestData, defaultArg metadataTimeout defaultMetadataTimeout)


    let rec getValue v  = 
        match v with
        | VInt8 x -> box x
        | VInt16 x -> box x
        | VInt32 x -> box x
        | VInt64 x -> box x
        | VSingle x -> box x
        | VDouble x -> box x
        | VDecimal x -> box x
        | VBoolean x -> box x
        | VString x -> box x
        | VArray x -> box (Array.map getValue x)
        | VStruct _x -> failwith "nyi: struct"
        | VMap x -> dict (x |> Array.map (fun (x,y) -> getValue x, getValue y)) |> box


    member __.RequestSent = event.Publish

    /// Dynamically read the table names from the Hive data store
    member __.GetTableNames () =
            match executeMetadataRequest (GetTableNames, None) with 
            | TableNames(tableNames) -> tableNames
            | data -> RuntimeHelpers.unexpected "GetTableNames" data

    /// Dynamically read the table names from the Hive data store
    member __.GetTablePartitionNames (tableName) =
            match executeMetadataRequest (GetTablePartitionNames(tableName), None) with 
            | TablePartitionNames(partitionNames) -> partitionNames
            | data -> RuntimeHelpers.unexpected "GetPartitionNames" data

    /// Dynamically read the table names from the Hive data store
    member __.GetTableSchema (tableName) =
        match executeMetadataRequest (GetTableSchema(tableName), None) with 
        | TableSchema(schema) -> schema
        | data -> RuntimeHelpers.unexpected "GetTableSchema" data

    /// Dynamically read the table description from the Hive data store.
    member __.GetTableDescription (tableName) = 
            match executeMetadataRequest (GetTableDescription(tableName),None) with
            | TableDescription(tableDesc,_tableHead) -> tableDesc
            | data -> RuntimeHelpers.unexpected "TableDescription" data

    member x.GetTable<'T> (tableName) = HiveTable<'T>(x, tableName)

    member x.GetTableUntyped (tableName) = HiveTable<HiveDataRow>(x, tableName)

    member x.ExecuteCommand (command, ?timeout) = 
        executeQueryRequest (ExecuteCommand(command), timeout) |> function 
            | CommandResult(text) -> text
            | data -> RuntimeHelpers.unexpected "Commmand" data

    /// Execute a single query value
    member internal x.ExecuteQueryValue (queryData, ?timeout) = 
        let _tableName, colData = colDataOfQuery queryData 
        let rows = 
            executeQueryRequest (GetDataFrame(queryData, colData), timeout) |> function 
                | DataFrame(rows) -> rows
                | data -> RuntimeHelpers.unexpected "GetDataFrame" data

        getValue rows.[0].[0] 
        
    /// Dynamically read the rows of a table from the Hive data store.
    member internal __.ExecuteQuery (queryData, rowType, ?timeout) : seq<obj> = 
        seq { 
            let _tableName, colData = colDataOfQuery queryData 
            let rows = 
                executeQueryRequest (GetDataFrame(queryData, colData), timeout) |> function 
                    | DataFrame(rows) -> rows
                    | data -> RuntimeHelpers.unexpected "GetDataFrame" data

            // If making F# record values, precompute a record constructor
            let maker = 
                if rowType = typeof<HiveDataRow> then 
                   None
                elif FSharpType.IsRecord rowType then 
                    // We don't need to permute the record fields here because F# quotations are already permuted.
                   Some (FSharpValue.PreComputeRecordConstructor(rowType,BindingFlags.NonPublic ||| BindingFlags.Public))
                elif FSharpType.IsUnion rowType && FSharpType.GetUnionCases(rowType).Length=1  then 
                   Some (FSharpValue.PreComputeUnionConstructor(FSharpType.GetUnionCases(rowType).[0],BindingFlags.NonPublic ||| BindingFlags.Public))
                elif FSharpType.IsTuple rowType then 
                   Some (FSharpValue.PreComputeTupleConstructor(rowType))
                else 
                   None //failwith (sprintf "unrecognized row type '%A'" rowType)

            for row in rows do
                let values = (colData, row) ||> Array.map2 (fun colData v -> (colData.HiveName,getValue v) )
                match maker with 
                | None -> yield HiveDataRow (dict values) |> box 
                | Some f -> yield f (Array.map snd values)
                    
        }

/// Represents a query of the Hive data store
and internal IHiveQueryable = 
    abstract DataContext : HiveDataContext
    abstract QueryData : HiveQueryData
    abstract TailOps : Expr list
    abstract Timeout : int<s> option

and internal IHiveTable = 
    abstract TableName : string 

and HiveTable<'T> internal (dataCtxt: HiveDataContext, tableName: string, colData: HiveColumnSchema[]) =
    inherit HiveQueryable<'T>(dataCtxt, Table(tableName, colData), [], None)

    new (dataCtxt: HiveDataContext, tableName: string) = 
        // Performance: it would be nice not to have to make this metadata request when creating the table
        let colData = dataCtxt.GetTableSchema(tableName).Columns
        new HiveTable<'T>(dataCtxt, tableName, colData)

    /// Get the table name of the Hive table
    member x.TableName = tableName
    
    /// Get the partition names for the Hive table
    member x.GetPartitionNames() = dataCtxt.GetTablePartitionNames(tableName)

    /// Get the schema for the Hive table
    member x.GetSchema() = dataCtxt.GetTableSchema(tableName)
    interface IHiveTable with 
        member x.TableName = tableName

#if GROUP_BY
and HiveGrouping<'Key,'T> internal (key: 'Key, dataCtxt: HiveDataContext, queryData: HiveQueryData, tailOps: Expr list, timeout: int<s> option) = 
    inherit HiveQueryable<'T>(dataCtxt, queryData, tailOps, timeout)
    member x.Key = key
#endif

and HiveQueryable<'T> internal (dataCtxt: HiveDataContext, queryData: HiveQueryData, tailOps: Expr list, timeout: int<s> option) = 

    member x.Run(?timeout) = 
        let rowType = 
            match tailOps with 
            | [] -> typeof<'T>
            | tailOp::_ -> 
                assert (tailOp.Type.IsGenericType)  // assert the tailOp is a function
                assert (tailOp.Type.GetGenericTypeDefinition() = typedefof<int -> int>) // assert the tailOp is a function
                let funTy = tailOp.Type.GetGenericArguments()
                let domTy,_ranTy = funTy.[0], funTy.[1]
                domTy
        let objs = dataCtxt.ExecuteQuery(queryData, rowType, ?timeout=timeout)
        (objs,tailOps) 
          ||> List.fold (fun objsInt tailOp ->
            // Compile the tailOp
            assert (tailOp.Type.IsGenericType)  // assert the tailOp is a function
            assert (tailOp.Type.GetGenericTypeDefinition() = typedefof<int -> int>) // assert the tailOp is a function
            let funTy = tailOp.Type.GetGenericArguments()
            let domTy,ranTy = funTy.[0], funTy.[1]
            let delTy = typedefof<Converter<_,_>>.MakeGenericType [| domTy; ranTy |]
            let delVar = Var("x",domTy)
            let tailOpDel = Expr.NewDelegate(delTy,[delVar],Expr.Application(tailOp,Expr.Var(delVar)))
            let tailOpExpression = Linq.RuntimeHelpers.LeafExpressionConverter.QuotationToExpression tailOpDel :?> LambdaExpression
            let del = tailOpExpression.Compile()
            objsInt |> Seq.map (fun obj -> del.DynamicInvoke [| obj |]))
          |> Seq.cast<'T>
          |> Seq.toArray

    interface IHiveQueryable with 
        member x.QueryData = queryData
        member x.DataContext = dataCtxt
        member x.TailOps = tailOps
        member x.Timeout = timeout

    override __.ToString() = sprintf "<Hive Query '%s'>" (HiveSchema.formatQuery queryData)
    member x.GetQueryString() = HiveSchema.formatQuery queryData

[<AutoOpen>]
module internal HiveQuery = 
    let run (hq: HiveQueryable<'T>) = hq.Run()
    let queryString (hq: HiveQueryable<'T>) = hq.GetQueryString()

[<AutoOpen>]
module internal QuotationUtils = 

    let (|Getter|_|) (prop: PropertyInfo) =
        match prop.GetGetMethod(true) with 
        | null -> None
        | v -> Some v

    let (|MacroReduction|_|) (p : Expr) = 
        match p with 
        | Applications(Lambdas(vs, body), args) when vs.Length = args.Length && List.forall2 (fun vs args -> List.length vs = List.length args) vs args -> 
            let tab = Map.ofSeq (List.concat (List.map2 List.zip vs args))
            let body = body.Substitute tab.TryFind 
            Some body

        // Macro
        | PropertyGet(None, Getter(MethodWithReflectedDefinition(body)), []) -> 
            Some body

        // Macro
        | Call(None, MethodWithReflectedDefinition(Lambdas(vs, body)), args) -> 
            let tab = Map.ofSeq (List.concat (List.map2 (fun (vs:Var list) arg -> match vs, arg with [v], arg -> [(v, arg)] | vs, NewTuple(args) -> List.zip vs args | _ -> List.zip vs [arg]) vs args))
            let body = body.Substitute tab.TryFind 
            Some body

        // Macro - eliminate 'let'. 
        //
        // Always eliminate these:
        //    - function definitions 
        //
        // Always eliminate these, which are representations introduced by F# quotations:
        //    - let v1 = v2
        //    - let v1 = tupledArg.Item*
        //    - let copyOfStruct = ...

        | Let(v, e, body) when (match e with 
                                | Lambda _ -> true
                                | Var _ -> true 
                                | TupleGet(Var tv, _) when tv.Name = "tupledArg" -> true 
                                | _ when v.Name = "copyOfStruct" && v.Type.IsValueType -> true
                                | _ -> false) ->
            let body = body.Substitute (fun v2 -> if v = v2 then Some e else None)
            Some body
        
        | _ -> None

    let rec existsSubQuotation f q = 
        f q || 
        match q with 
        | Quotations.ExprShape.ShapeCombination(_op,xs) -> xs |> List.exists (existsSubQuotation f)
        | Quotations.ExprShape.ShapeLambda(_v,b) -> existsSubQuotation f b
        | Quotations.ExprShape.ShapeVar _ -> false

    let (|NestedQuery|_|) q = 
        match q with 
        | Application(Lambda(_builderVar,Call(_builderVar2,mi,[Quote(queryQuotation)])),_builderExpr) when mi.Name = "Run" ->  Some(queryQuotation)
        | _ -> None
    
    let isNullableTy (ty:Type) = ty.IsGenericType && ty.GetGenericTypeDefinition() = typedefof<Nullable<_>>

    /// A column in an intermediate table gets labelled as 'required' if it is a value type
    /// and the F# type for the selected expression is not nullable.
    let isRequiredTy (ty:Type) = ty.IsValueType && not (isNullableTy ty)

type internal OptionBuilder() = 
    member inline __.Bind(x,f) = match x with None -> None | Some v -> f v
    member inline __.Return x = Some x
    member inline __.ReturnFrom x = x

type internal HiveExpressionTranslator(decodeNestedQuery, dataCtxt:HiveDataContext, tableName, tableVar:Var, colData, auxVars, auxExprs, allowEvaluation: bool) = 
    let option = OptionBuilder()
    let (|Member|_|) (s:string) (mi:MemberInfo) = if mi.Name = s then Some() else None
    let (|StringMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "System.String") -> Some() | _ -> None
    let (|DateTimeMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "System.DateTime") -> Some() | _ -> None
    let (|DecimalMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "System.Decimal") -> Some() | _ -> None
    let (|MathMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "System.Math") -> Some() | _ -> None
    let (|OperatorsMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators") -> Some() | _ -> None
    let (|NullableOperatorsMember|_|) s mi = match mi with Member s when (mi.DeclaringType.FullName = "Microsoft.FSharp.Linq.NullableOperators") -> Some() | _ -> None
    let (|DecimalMathOrOperatorsMember|_|) s mi = 
        match mi with 
        | DecimalMember s -> Some()
        | OperatorsMember s -> Some()
        | MathMember s -> Some()
        |  _ -> None

    let EAtom s = EFunc(s,[], DAtom)
    let EString s = EVal (VString s, DString)
    let EInt32 s = EVal (VInt32 (Value s), DInt32)

        
    let RelOp(e1, op, e2) = EBinOp(e1, op, e2, DBoolean)
    let (&&.) e1 e2 = RelOp(e1, "AND", e2)
    let (||.) e1 e2 = RelOp(e1, "OR", e2)
    let NotOp e1 = EFunc("NOT", [ e1 ], DBoolean)
    let (+.) e1 e2 = EBinOp(e1, "+", e2, typeOfExpr e1)
    let ( *. ) e1 e2 = EBinOp(e1, "*", e2, typeOfExpr e1)
    let (%.) e1 e2 = EBinOp(e1, "%", e2, typeOfExpr e1)
    let (/.) e1 e2 = EBinOp(e1, "/", e2, typeOfExpr e1)
    let (-.) e1 e2 = EBinOp(e1, "-", e2, typeOfExpr e1)

    let concat es = EFunc("CONCAT", es, DString)
    let colTableByName = dict [ for col in colData  -> col.HiveName, col.HiveType ]
    do assert (List.length auxVars = List.length auxExprs)
    let auxMap = dict (List.zip auxVars auxExprs)


    let rec tryConvExpr e = 
      option {
        match e with 

        // 'select x' where 'x' is a whole table
        | Var v2 when tableVar = v2 -> return ETable(tableName, colData)

        // 'x' where 'x' is an auxiliary let-bound or other bound variable
        | Var v2 when auxMap.ContainsKey v2 -> return! tryConvExpr auxMap.[v2]

        // Property access when an F# record type is used as the intermediate schema
        | PropertyGet(Some (Var v2), propInfo, []) when tableVar = v2 && Reflection.FSharpType.IsRecord propInfo.DeclaringType && colTableByName.ContainsKey propInfo.Name -> 
            let propName = propInfo.Name
            let hiveType = colTableByName.[propName]
            return EColumn (propName, hiveType) 

        // Property access when an F# record type is used as the intermediate schema
        | TupleGet(Var v2, n) when tableVar = v2 && (colTableByName.ContainsKey ("Item" + string (n+1))) -> 
            let propName = "Item" + string (n+1)
            let hiveType = colTableByName.[propName]
            return EColumn (propName, hiveType) 

        // Property access on an erased type
        | Call(Some (Var v2), Member "GetValue", [String hiveName]) when tableVar = v2 && colTableByName.ContainsKey hiveName -> 
            let hiveType = colTableByName.[hiveName]
            return EColumn (hiveName, hiveType) 

#if GROUP_BY
        // Property access g.Key for a group
        | PropertyGet(Some (Var v2), propInfo, []) when tableVar = v2 && propInfo.Name = "Key" && propInfo.DeclaringType.Name = "HiveGrouping`2" -> 
            return EColumn ("*", colTableByName.["$key"]) // SELECT * FROM table GROUP BY keyExpr;  <-- this selects the key (actually, no it doesn't, see comments for #if GROUP_BY below)

        // Detect a nested query. The only nested queries we allow are iterating a group 
        // which results from 'GroupBy'
        | NestedQuery(queryQuotation) when colTableByName.ContainsKey "$group" -> 

            let (_hqCtxt:HiveDataContext), hqQueryData, _hqTail, _hqTimeout, _hqAuxExprsF  = 
                let groupColData = 
                    match colTableByName.["$group"] with 
                    | DTable cols -> cols
                    | _ -> failwith "unexpected group type"

                decodeNestedQuery (Some (tableVar,dataCtxt,tableName,groupColData)) false queryQuotation
            // TODO: check the query context and table are the same
            // TODO: check there is no timeout
            return EQuery(hqQueryData)
#endif

        | Int32 d  -> return EVal (VInt32 (Value d), DInt32) 
        | Int64 d  -> return EVal (VInt64 (Value d), DInt64) 
        | Single d -> return EVal (VSingle (Value d), DSingle)  
        | Double d -> return EVal (VDouble (Value d), DDouble) 
        | String null -> return EAtom "null"
        | String d -> return EString d 
        | Bool d   -> return EVal (VBoolean (Value d), DBoolean) 
        // Detect constant decimal creations, which get encoded in F#
        | Call(None,mi,[Int32 d1; Int32 d2; Int32 d3; Bool d4; Byte d5]) when mi.Name = "MakeDecimal" -> 
             return EVal (VDecimal (Value (Decimal(d1,d2,d3,d4,d5))), DDecimal) 

        // Nullable() --> null
        | DefaultValue _ -> return EAtom("null")
        // Nullable(x) --> x
        | NewObject(cinfo,[arg]) when isNullableTy cinfo.DeclaringType -> return! tryConvExpr arg
        // x.Value --> x
        | PropertyGet(Some obj,pinfo,_) when pinfo.Name = "Value" && isNullableTy pinfo.DeclaringType -> return! tryConvExpr obj


        // s.Length
        | PropertyGet(Some obj,StringMember "Length",[])         -> return! unop obj (fun objH -> EFunc("LENGTH",[objH], DInt32))
        // dateTime.Day ...

        // TODO: what if user's input contains '%', this is not mentioned in the Hive docs
        // string.StartsWith("a") ..
        | Call(Some obj,StringMember "StartsWith",[arg1])       -> return! binop obj arg1 (fun objH arg1H -> RelOp(objH,"LIKE", concat [arg1H; EString "%"]))
        | Call(Some obj,StringMember "EndsWith"  ,[arg1])       -> return! binop obj arg1 (fun objH arg1H -> RelOp(objH,"LIKE", concat [EString "%"; arg1H]))
        | Call(Some obj,StringMember "Contains"  ,[arg1])       -> return! binop obj arg1 (fun objH arg1H -> RelOp(objH,"LIKE",concat [EString "%"; arg1H; EString "%"]) )

        // s.ToUpper() ..
        | Call(Some obj,StringMember "ToUpper"   ,[])           -> return! unop obj (fun objH -> EFunc("UPPER",[objH], DString))
        | Call(Some obj,StringMember "ToLower"   ,[])           -> return! unop obj (fun objH -> EFunc("LOWER",[objH], DString))

        // System.String.IsNullOrEmpty(stringValue) ..
        | Call(None,StringMember "IsNullOrEmpty" ,[obj])        -> return! unop obj (fun objH -> RelOp(RelOp(objH,"IS",EAtom("NULL")),"OR",RelOp(objH,"=",EString "")))


        // s.Replace("a","b") ..

        | Call(Some obj,StringMember "Replace"   ,[arg1; arg2]) -> return! triop obj arg1 arg2 (fun objH arg1H arg2H -> EFunc("REPLACE",[objH;arg1H;arg2H], DString))

        // s.Substring(3)
        | Call(Some obj,StringMember "Substring" ,[arg1])       -> return! binop obj arg1 (fun objH arg1H -> EFunc("SUBSTRING",[objH;arg1H +. EInt32 1;EInt32 8000], DString))
        // s.Substring(3,5)
        | Call(Some obj,StringMember "Substring" ,[arg1;arg2])  -> return! triop obj arg1 arg2 (fun objH arg1H arg2H -> EFunc("SUBSTRING",[objH;arg1H +. EInt32 1;arg2H], DString))

        // s.Remove(3) 
        | Call(Some obj,StringMember "Remove"    ,[arg1])       -> return! binop obj arg1 (fun objH arg1H -> EFunc("STUFF",[objH;arg1H +. EInt32 1;EInt32 8000], DString))
        // s.Remove(3,5) 
        | Call(Some obj,StringMember "Remove"    ,[arg1;arg2])  -> return! triop obj arg1 arg2 (fun objH arg1H arg2H -> EFunc("STUFF",[objH;arg1H +. EInt32 1;arg2H], DString))
        // 
        | Call(Some obj,StringMember "Trim"      ,[])           -> return! unop obj (fun objH -> EFunc("RTRIM",[EFunc("LTRIM",[objH],DString)],DString))
        | Call(Some obj,StringMember "TrimEnd"   ,[])           -> return! unop obj (fun objH -> EFunc("RTRIM",[objH],DString))
        | Call(Some obj,StringMember "TrimStart" ,[])           -> return! unop obj (fun objH -> EFunc("LTRIM",[objH],DString))

        // a && b   
        | AndAlso(arg1, arg2) -> return! binop arg1 arg2 (&&.)
        // a || b   
        | OrElse(arg1, arg2) -> return! binop arg1 arg2 (||.)
        // not a 
        | Call(None,OperatorsMember "Not"     ,[arg1]) -> return! unop arg1 NotOp

        // a + b   (for any type)
        | Call(None,Member "op_Addition"     ,[arg1; arg2]) -> return! binop arg1 arg2 (+.)
        // a - b   (for any type)
        | Call(None,Member "op_Subtraction"  ,[arg1; arg2]) -> return! binop arg1 arg2 (-.)
        // a * b   (for any type)
        | Call(None,Member "op_Multiply"     ,[arg1; arg2]) -> return! binop arg1 arg2 ( *. )
        // a / b   (for any type)
        | Call(None,Member "op_Division"     ,[arg1; arg2]) -> return! binop arg1 arg2 (/.)
        // a % b   (for any type)
        | Call(None,Member "op_Modulus"      ,[arg1; arg2]) -> return! binop arg1 arg2 (%.) 
        // -a   (for any type)
        | Call(None,Member "op_UnaryNegation",[arg1]) -> return! unop arg1 (fun arg1H -> EFunc("-",[arg1H],typeOfExpr arg1H))
        // a ** b   (for any type)
        | Call(None,Member "op_Exponentiation",[arg1; arg2])-> return! binop arg1 arg2 (fun arg1H arg2H -> EFunc("POWER",[arg1H; arg2H], typeOfExpr arg1H)) 

        // System.Decimal.Add(a,b) // note why not do it with '+'??
        | Call(None,DecimalMember "Add"      ,[arg1; arg2]) -> return! binop arg1 arg2 (+.)
        | Call(None,DecimalMember "Subtract" ,[arg1; arg2]) -> return! binop arg1 arg2 (-.)
        | Call(None,DecimalMember "Multiply" ,[arg1; arg2]) -> return! binop arg1 arg2 ( *. )
        | Call(None,DecimalMember "Divide"   ,[arg1; arg2]) -> return! binop arg1 arg2 (/.) 
        | Call(None,DecimalMember "Remainder",[arg1; arg2]) -> return! binop arg1 arg2 (%.)
        | Call(None,DecimalMember "Negate"   ,[arg1])       -> return! unop arg1 (fun arg1H -> EFunc("-",[arg1H],typeOfExpr arg1H))

        // TODO: use the correct translation of all conversions. For now assuming the identity function.
        
        // double x
        | Call(None,OperatorsMember "ToDouble",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DDouble))
        // decimal x
        | Call(None,OperatorsMember "ToDecimal",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DDecimal))
        // single x
        | Call(None,OperatorsMember "ToSingle",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DSingle))
        // sbyte x
        | Call(None,OperatorsMember "ToSByte",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DInt8))
        // int16 x
        | Call(None,OperatorsMember "ToInt16",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DInt16))
        // int32 x
        | Call(None,OperatorsMember "ToInt32",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DInt32))
        // int64 x
        | Call(None,OperatorsMember "ToInt64",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("$id",[arg1H], DInt64))

        // ceil x
        | Call(None,DecimalMathOrOperatorsMember "Ceiling",[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("CEILING",[arg1H],typeOfExpr arg1H))  
        // floor x
        | Call(None,DecimalMathOrOperatorsMember "Floor"  ,[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("FLOOR",[arg1H], typeOfExpr arg1H))
        // round x
        | Call(None,DecimalMathOrOperatorsMember "Round"  ,[arg1])  -> return! unop arg1 (fun arg1H -> EFunc("ROUND",[arg1H], typeOfExpr arg1H)) 
        // System.Decimal.Round(x, howManyDecimalsToRoundTo)
        | Call(None,DecimalMathOrOperatorsMember "Round"  ,[arg1; arg2]) when arg2.Type = typeof<int> -> 
                                                                       return! binop arg1 arg2 (fun arg1H arg2H -> EFunc("ROUND",[arg1H; arg2H], typeOfExpr arg1H)) 
        // truncate x
        | Call(None,DecimalMathOrOperatorsMember "Truncate",[arg1]) -> return! unop arg1 (fun arg1H -> EFunc("ROUND",[arg1H; EInt32 0; EInt32 1], typeOfExpr arg1H) )

        // abs x, acos x, asin x, ... etc.
        | Call(None,((DecimalMathOrOperatorsMember "Abs"
                     |DecimalMathOrOperatorsMember "Acos"
                     |DecimalMathOrOperatorsMember "Asin"
                     |DecimalMathOrOperatorsMember "Atan"
                     |DecimalMathOrOperatorsMember "Cos"
                     |DecimalMathOrOperatorsMember "Exp"
                     |DecimalMathOrOperatorsMember "Log10"
                     |DecimalMathOrOperatorsMember "Sin"
                     |DecimalMathOrOperatorsMember "Tan"
                     |DecimalMathOrOperatorsMember "Sqrt"
                     |DecimalMathOrOperatorsMember "Sign"
                     |DecimalMathOrOperatorsMember "Ceiling"
                     |DecimalMathOrOperatorsMember "Floor") as m) ,[arg1]) -> 
            return! unop arg1 (fun arg1H -> EFunc(m.Name.ToUpper(),[arg1H], typeOfExpr arg1H))

        // atan2 x y 
        | Call(None,DecimalMathOrOperatorsMember "Atan2",[arg1; arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> EFunc("ATAN2",[arg1H; arg2H], typeOfExpr arg1H))
        // log x 
        | Call(None,DecimalMathOrOperatorsMember "Log"  ,[arg1])       -> return! unop arg1 (fun arg1H -> EFunc("LOG10",[arg1H], typeOfExpr arg1H))


#if DATETIME_SUPPORT

    //let DDate = DString // TODO: check me

// Note, this is a long way from working
        
        | PropertyGet(Some obj,DateTimeMember "Day",[])         -> return! unop obj (fun objH -> EFunc("DAY",[objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Month",[])       -> return! unop obj (fun objH -> EFunc("MONTH",[objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Year",[])        -> return! unop obj (fun objH -> EFunc("YEAR",[objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Hour",[])        -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "hour";objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Minute",[])      -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "minute";objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Second",[])      -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "second";objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "Millisecond",[]) -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "millisecond";objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "DayOfWeek",[])   -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "weekday";objH], DInt32))
        | PropertyGet(Some obj,DateTimeMember "DayOfYear",[])   -> return! unop obj (fun objH -> EFunc("DATEPART",[EAtom "dayofyear";objH], DInt32))
        | Call(None,DateTimeMember "op_Subtraction",[arg1; arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> EFunc("DATEDIFF",[arg1H;arg2H], DDate))
        | Call(Some obj,DateTimeMember "AddYears"       ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "YYYY";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddMonths"      ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "MM";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddDays"        ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "DD";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddHours"       ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "HH";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddMinutes"     ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "MI";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddSeconds"     ,[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "SS";objH;arg1H], DDate))
        | Call(Some obj,DateTimeMember "AddMilliseconds",[arg1]) -> return! binop obj arg1 (fun objH arg1H -> EFunc("DATEADD",[EAtom "MS";objH;arg1H], DDate))

        // new System.DateTime(31,12,2010)
        | NewObject(ci, [arg1;arg2;arg3]) when ci.DeclaringType.FullName = "System.DateTime" ->
                    // TODO: check this one
              return! triop arg1 arg2 arg3 (fun arg1H arg2H arg3H -> 
                  EFunc("Convert", [ EAtom "DateTime"; 
                                  concat [EFunc("Convert",[EAtom "nvarchar"; arg1H], DString); EString "/"
                                          EFunc("Convert",[EAtom "nvarchar"; arg2H], DString); EString "/"
                                          EFunc("Convert",[EAtom "nvarchar"; arg3H], DString)]], DDate))

        // new System.DateTime(2010,12,31,23,59,59)
        | NewObject(ci, [arg1;arg2;arg3;arg4;arg5;arg6]) when ci.DeclaringType.FullName = "System.DateTime" ->  
              return! sixop arg1 arg2 arg3 arg4 arg5 arg6 (fun arg1H arg2H arg3H arg4H arg5H arg6H -> 
                    EFunc("Convert", [ EAtom "DateTime"; 
                                       concat [EFunc("Convert",[EAtom "nvarchar"; arg1H], DString); EString "/"
                                               EFunc("Convert",[EAtom "nvarchar"; arg2H], DString); EString "/"
                                               EFunc("Convert",[EAtom "nvarchar"; arg3H], DString); EString " "
                                               EFunc("Convert",[EAtom "nvarchar"; arg4H], DString); EString ":"
                                               EFunc("Convert",[EAtom "nvarchar"; arg5H], DString); EString ":"
                                               EFunc("Convert",[EAtom "nvarchar"; arg6H], DString)]], DDate))

#endif

        // a = b (any type)
        | Call(None,OperatorsMember "op_Equality"          ,[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "=", arg2H))
        // a < b (any type)
        | Call(None,OperatorsMember "op_LessThan"          ,[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "<", arg2H))
        // a > b (any type)
        | Call(None,OperatorsMember "op_GreaterThan"       ,[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, ">", arg2H))
        | Call(None,OperatorsMember "op_LessThanOrEqual"   ,[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "<=", arg2H))
        | Call(None,OperatorsMember "op_GreaterThanOrEqual",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, ">=", arg2H))
        // a <> b (any type)
        | Call(None,OperatorsMember "op_Inequality"        ,[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "!=", arg2H))


        // Nullable operators x ?= y etc.
        // a ?= b (any type)
        | Call(None,NullableOperatorsMember "op_QmarkEquals",[arg1;arg2]) 
        // a ?=? b (any type)
        | Call(None,NullableOperatorsMember "op_QmarkEqualsQmark",[arg1;arg2]) 
        // a =? b (any type)
        | Call(None,NullableOperatorsMember "op_EqualsQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "=", arg2H))

        // a ?< b (any type)
        | Call(None,NullableOperatorsMember "op_QmarkLess",[arg1;arg2]) 
        // a ?<? b (any type)
        | Call(None,NullableOperatorsMember "op_QmarkLessQmark",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_LessQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "<", arg2H))

        | Call(None,NullableOperatorsMember "op_QmarkGreater",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkGreaterQmark",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_GreaterQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, ">", arg2H))

        | Call(None,NullableOperatorsMember "op_QmarkLessEquals",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkLessEqualsQmark",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_LessEqualsQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "<=", arg2H))

        | Call(None,NullableOperatorsMember "op_QmarkGreaterEquals",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkGreaterEqualsQmark",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_GreaterEqualsQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, ">=", arg2H))

        | Call(None,NullableOperatorsMember "op_QmarkLessGreater",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkLessGreaterQmark",[arg1;arg2]) 
        | Call(None,NullableOperatorsMember "op_LessGreaterQmark",[arg1;arg2]) -> return! binop arg1 arg2 (fun arg1H arg2H -> RelOp(arg1H, "!=", arg2H))

        // Nullable operators x ?+ y etc.
        | Call(None,NullableOperatorsMember "op_QmarkPlus",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkPlusQmark",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_PlusQmark",[arg1; arg2]) -> return! binop arg1 arg2 (+.)

        | Call(None,NullableOperatorsMember "op_QmarkMinus",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkMinusQmark",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_MinusQmark",[arg1; arg2]) -> return! binop arg1 arg2 (-.)

        | Call(None,NullableOperatorsMember "op_QmarkMultiply",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkMultiplyQmark",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_MultiplyQmark",[arg1; arg2]) -> return! binop arg1 arg2 ( *. )

        | Call(None,NullableOperatorsMember "op_QmarkDivide",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkDivideQmark",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_DivideQmark",[arg1; arg2]) -> return! binop arg1 arg2 (/.)

        // a ?% b (any type)
        | Call(None,NullableOperatorsMember "op_QmarkPercent",[arg1; arg2]) 
        | Call(None,NullableOperatorsMember "op_QmarkPercentQmark",[arg1; arg2]) 
        // a %? b (any type)
        | Call(None,NullableOperatorsMember "op_PercentQmark",[arg1; arg2]) -> return! binop arg1 arg2 (%.)


        | QuotationUtils.MacroReduction reduced -> return! tryConvExpr reduced


        | _ -> 
            let n = if allowEvaluation then try Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation e with _ -> null else null
            match n with 
            | :? Int32 as d -> return EInt32 d 
            | :? Int64  as d -> return EVal (VInt64 (Value d), DInt64) 
            | :? Single  as d -> return EVal (VSingle (Value d), DSingle) 
            | :? Double  as d -> return EVal (VDouble (Value d), DDouble) 
            | :? Decimal as d -> return EVal (VDecimal (Value d), DDecimal) 
            | :? String  as s -> return EString s  
            | :? Boolean as b -> return EVal (VBoolean (Value  b), DBoolean) 
            | _obj -> return! None
        }
    and unop arg1 f = 
        option { 
            let! arg1H = tryConvExpr arg1 
            return f arg1H 
        }
    and binop arg1 arg2 f = 
        option { 
            let! arg1H = tryConvExpr arg1 
            let! arg2H = tryConvExpr arg2 
            return f arg1H arg2H
        }
    and triop arg1 arg2 arg3 f = 
        option { 
            let! arg1H = tryConvExpr arg1 
            let! arg2H = tryConvExpr arg2 
            let! arg3H = tryConvExpr arg3
            return f arg1H arg2H arg3H
        }
    and sixop arg1 arg2 arg3 arg4 arg5 arg6 f = 
        option { 
            let! arg1H = tryConvExpr arg1 
            let! arg2H = tryConvExpr arg2 
            let! arg3H = tryConvExpr arg3
            let! arg4H = tryConvExpr arg4
            let! arg5H = tryConvExpr arg5 
            let! arg6H = tryConvExpr arg6
            return f arg1H arg2H arg3H arg4H arg5H arg6H
        }
    let convExpr x = 
        match tryConvExpr x with 
        | Some res -> res
        | None -> failwithf "Unrecognized expression: %A" x


    member x.ConvertExpression c = convExpr c
    member x.RecognizeExpression c = 
        let res = tryConvExpr c  |> Option.isSome
        //if res then printfn "recognized %A" c
        //else printfn "did not recognize %A" c
        res

type HiveQueryBuilder() = 

    /// Detect 'select' queries where we have not had a 'where', 'count' or any other server-side operation (apart from 'limit').
    /// This causes the query translation to prefer a pure 'select *' plus a client-side tail operation, which is much faster
    /// because no map/reduce job is scheduled.
    let rec isSimpleSelect q = 
        match q with 
        | Table _ -> true
        | Distinct _ | GroupBy _ | AggregateOpBy _ | Count _ | Where _ -> false
        | Limit(hq,_) | Select(hq,_) | TableSample(hq,_,_) -> isSimpleSelect hq 

    let normalize auxVars auxExprs (e:Expr) = 
        assert (List.length auxVars = List.length auxExprs)
        let auxMap = dict (List.zip auxVars auxExprs)
        e.Substitute(fun v -> if auxMap.ContainsKey v then Some auxMap.[v] else None)

    // canTail - can we accept a 'select' with tail ops
    let rec decodeQuery nestedTableOpt canTail q = 
        match q with 
        | Call(_,mi,[hq;Lambdas([(tableVar::auxVars)],selectorBody)]) when mi.Name = "Select" -> 
            decodeSelect nestedTableOpt canTail (hq,tableVar,auxVars,selectorBody)

        // newTable tableName expr
        // writeDistributedFile tableName expr
        //
        // Note: writeHeadNodeFile is disabled, see note below
        | Call(_,mi,[hq;targetTableNameExpr;Lambdas([tableVar::auxVars],selectorBody)]) when mi.Name = "NewTable" || mi.Name = "WriteHeadNodeFile" || mi.Name = "WriteDistributedFile" -> 
            let (hqCtxt:HiveDataContext), hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeSelect nestedTableOpt false (hq,tableVar,auxVars,selectorBody)
            // Evaluate the name of the table
            let targetTableName = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation targetTableNameExpr :?> string
            let _tableName,colData = colDataOfQuery hqQueryData
            if mi.Name = "NewTable" then 
                try 
                  hqCtxt.ExecuteCommand("DROP TABLE " + targetTableName) |> ignore
                with _ -> ()

                let columns = 
                    [ for c in colData -> 
                          c.HiveName + " " + HiveSchema.formatHiveType c.HiveType + 
                          " COMMENT 'The column " + c.HiveName + " in the table " + targetTableName + " " + HiveSchema.formatHiveComment c.IsRequired + "'" 
                    ] 
                    |> String.concat ","

                hqCtxt.ExecuteCommand("CREATE TABLE " + targetTableName + "(" + columns + ")", ?timeout=hqTimeout) |> ignore
                let queryText = HiveSchema.formatQuery hqQueryData
                hqCtxt.ExecuteCommand("INSERT OVERWRITE TABLE " + targetTableName + " " + queryText, ?timeout=hqTimeout) |> ignore
                let _tableName, colData = colDataOfQuery hqQueryData
                hqCtxt, Table(targetTableName, colData), hqTail, None, hqAuxExprsF 
            else 
                let queryText = HiveSchema.formatQuery hqQueryData
                hqCtxt.ExecuteCommand("INSERT OVERWRITE " + (if mi.Name = "WriteDistributedFile" then "" else "LOCAL ") + "DIRECTORY '" + targetTableName + "' " + queryText, ?timeout=hqTimeout) |> ignore
                hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF 
            

        // writeRows (table.NewRow(....))
        // insertRows (table.NewRow(....))
        | Call(_,mi,[hq;Lambdas([tableVar::auxVars],rowDescription)]) when mi.Name = "WriteRows" || mi.Name = "InsertRows" || mi.Name = "WritePartition" || mi.Name = "InsertPartition" -> 
            let hqCtxt, hqQueryData, (hqTail:Expr list), hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            let hqAuxExprs = hqAuxExprsF tableVar
            let targetTable, colArgs =
                let rec look t = 
                    match t with 
                    | NewObject(_ci,[String targetTable; NewArray(_,elems)]) -> 
                        let colArgs = elems |> List.map (function NewTuple [String colName; Coerce(colArg,_); Bool colIsPartitionKey] -> (colName, colArg, colIsPartitionKey)
                                                                | x -> failwithf "unexpected form for row contents in 'WriteRows/InsertRows': %A " x)
                        targetTable, colArgs
                    | Coerce(t2,_) -> look t2
                    | Let(v,e,b) -> look (b.Substitute(fun v2 -> if v = v2 then Some e else None))
                    | _ -> failwithf "unexpected form for row description in 'WriteRows/InsertRows' :\n%A" t
                look rowDescription
            if not hqTail.IsEmpty then failwith "unexpected selection before 'writeRows'"
            
            let tableName, colData = colDataOfQuery hqQueryData
 
            let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
            let partitionKeys, trueColArgs = 
                colArgs |> List.partition (fun (_colName, _colExpr, colIsPartitionKey) -> colIsPartitionKey)
            
            if mi.Name = "WriteRows" && not partitionKeys.IsEmpty then failwith "use 'writePartition' to write to a partition of a partitioned table"
            if mi.Name = "WritePartition" && partitionKeys.IsEmpty then failwith "use 'writeRows' to write to a non-partitioned table"
            // e.g. PARTITION(loadKey1='2013-02-12', loadKey2='Iris-setosa')
            let partitionKeysText = 
                match partitionKeys with 
                | [] -> ""
                | _ ->
                   "PARTITION(" + 
                     (partitionKeys
                        |> List.map (fun (colName, colExpr, _colIsPartitionKey) -> 
                            let colExprConv = converter.ConvertExpression colExpr
                            let exprText = HiveSchema.formatExpr colExprConv
                            colName + "=" + exprText)
                        |> String.concat ",") + 
                     ") "
            let colExprs = 
                trueColArgs |> List.map (fun (colName, colExpr, _colIsPartitionKey) -> (colName, converter.ConvertExpression colExpr, isRequiredTy colExpr.Type)) 
                            |> List.toArray
            let hqQueryDataWithSelect = Select(hqQueryData, colExprs)
            let command = if mi.Name = "InsertRows" || mi.Name = "InsertPartition" then "INSERT INTO" else "INSERT OVERWRITE" 

            let queryText = HiveSchema.formatQuery hqQueryDataWithSelect 
            hqCtxt.ExecuteCommand(command + " TABLE " + targetTable + " " + partitionKeysText + queryText, ?timeout=hqTimeout) |> ignore
            hqCtxt, hqQueryData, hqTail, None, hqAuxExprsF 

        // where expr
        | Call(_,mi,[hq;Lambdas([tableVar::auxVars],pred)]) when mi.Name = "Where" -> 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            let hqAuxExprs = hqAuxExprsF tableVar
            let tableName, colData = colDataOfQuery hqQueryData
            let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
            let queryData = HiveQueryData.Where(hqQueryData, converter.ConvertExpression pred)
            hqCtxt, queryData, hqTail, hqTimeout, hqAuxExprsF

        // This recognizes for ... yield which may include additional 'let'. This arises whenever
        // internal 'let' expressions are used, e.g.
        //     
        //  hiveQuery { for x in ctxt.abalone do
        //              let avg = 1.0 
        //              let avg2 = 2.0 
        //              select (x.Name, avg + avg2) }
        //
        // Somewhat unexpectedly, the 'let' bind towards the 'for', in the sense that the construct becomes
        // this:
        //
        //  Select("for x in ctxt.abalone do
        //          let avg = 1.0 
        //          let avg2 = 2.0 
        //          yield (x,avg,avg2)"
        //         (fun x,avg,avg2 -> select (x.Name, avg + avg2)))
        //
        // The general pattern is For(..., Lambdas(Let..., Let(newVar,newExpr, ... Yield(...newVar)))
        | Call(_,mi1,[hq;Lambdas([tableVar::auxVars],forLoopBody)]) when mi1.Name = "For"  -> 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt canTail hq
            let hqAuxExprs = hqAuxExprsF tableVar
            let forLoopBodyNorm = normalize auxVars hqAuxExprs forLoopBody
            let rec loop e = 
                match e with 
                | Let(newAuxVar, newAuxExpr, e2) -> 
                    loop (e2.Substitute (fun v2 -> if newAuxVar = v2 then Some newAuxExpr else None))
                | Call(_,mi2,[NewTuple (Var tableVar2::finalAuxExprs)]) when tableVar = tableVar2 && mi2.Name = "Yield" -> 
                     (fun tableVar3 -> finalAuxExprs |> List.map (normalize [tableVar2] [Expr.Var tableVar3]))
                | Call(_,mi2,[Var tableVar2]) when tableVar = tableVar2 && mi2.Name = "Yield" -> 
                     (fun _ -> [])
                | e -> failwithf "unrecognized construct in 'for' expression: %A" e
            let newAuxExprs = loop forLoopBodyNorm
            hqCtxt, hqQueryData, hqTail, hqTimeout, newAuxExprs

#if GROUP_BY
        // This recognizes the 'groupBy ... into g' pattern
        | Call(_,mi2,[hq;Lambdas([tableVar::auxVars],keySelector)]) when mi2.Name = "GroupBy" -> 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            let hqAuxExprs = hqAuxExprsF tableVar
            let tableName, colData = colDataOfQuery hqQueryData
            let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
            let groupKeyExpr = converter.ConvertExpression keySelector
            hqCtxt, HiveQueryData.GroupBy(hqQueryData, groupKeyExpr), hqTail, hqTimeout, (fun _ -> [])
#endif

        // take numElements
        | Call(_,mi,[hq;nExpr]) when mi.Name = "Take" -> 
            let n = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation nExpr :?> int
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt canTail hq
            hqCtxt, HiveQueryData.Limit(hqQueryData, n), hqTail , hqTimeout, hqAuxExprsF 

        // distinct
        | Call(_,mi,[hq]) when mi.Name = "Distinct" -> 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            hqCtxt, HiveQueryData.Distinct(hqQueryData), hqTail , hqTimeout, hqAuxExprsF 

        // count
        | Call(_,mi,[hq]) when mi.Name = "Count" -> 
            let hqCtxt, hqQueryData, _hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            // Throw away _hqTail, e.g. select-then-count
            hqCtxt, HiveQueryData.Count(hqQueryData), [], hqTimeout, hqAuxExprsF 

        // sumBy expr
        // sumByNullable expr
        // ...
        | Call(_,mi,[hq;Lambdas([tableVar::auxVars],selector)]) 
                                                    when mi.Name = "SumBy" || mi.Name = "SumByNullable" || 
                                                         mi.Name = "MaxBy" || mi.Name = "MaxByNullable" || 
                                                         mi.Name = "MinBy" || mi.Name = "MinByNullable" || 
                                                         mi.Name = "AverageBy" || mi.Name = "AverageByNullable"  -> 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq
            let hqAuxExprs = hqAuxExprsF tableVar
            let tableName, colData = colDataOfQuery hqQueryData
            let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
            let selectorExpr = converter.ConvertExpression selector
            if not hqTail.IsEmpty then failwith ("unexpected selection before '" + mi.Name + "'")
            let op = 
                match mi.Name with 
                | "SumBy" | "SumByNullable" -> "SUM"
                | "MaxBy" | "MaxByNullable" -> "MAX"
                | "MinBy" | "MinByNullable" -> "MIN"
                | "AverageBy" | "AverageByNullable" -> "AVG"
                | _ -> failwith "unreachable"
            hqCtxt, HiveQueryData.AggregateOpBy(op,hqQueryData, selectorExpr, isRequiredTy selector.Type), [], hqTimeout, hqAuxExprsF 

        // timeout timeoutInSeconds
        | Call(_,mi,[hq;nExpr]) when mi.Name = "Timeout" -> 
            let n = (Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation nExpr :?> int) * 1<s>
            let hqCtxt, hqQueryData, hqTail, _hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt canTail hq
            hqCtxt, hqQueryData, hqTail, Some n, hqAuxExprsF 

        // sampleBucket bucketNumber numberOfBuckets
        | Call(_,mi,[hq;nExpr1;nExpr2]) when mi.Name = "SampleBucket" -> 
            let n1 = (Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation nExpr1 :?> int) 
            let n2 = (Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation nExpr2 :?> int) 
            let hqCtxt, hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt canTail hq
            hqCtxt, HiveQueryData.TableSample(hqQueryData,n1,n2), hqTail, hqTimeout, hqAuxExprsF

        // Match a table variable for a group, used in nested queries, e.g. 'g' in 'for x in g' in this:
        //
        //   hiveQuery { for x in ctxt.abalone do 
        //               groupBy x.sex into g
        //               select (g.Key,hiveQuery { for x in g do averageBy x.height }) } 
        //
        | Coerce(hq,_) -> decodeQuery nestedTableOpt canTail hq
        | Var v when (match nestedTableOpt with Some (nestedTableVar,_,_,_) -> v = nestedTableVar | None -> false) ->
            match nestedTableOpt with 
            | Some (_nestedTableVar,nestedTableCtxt,nestedTableName,nestedTableColData) -> 
                nestedTableCtxt, HiveQueryData.Table(nestedTableName,nestedTableColData), [], None, (fun _ -> [])
            | _ -> failwith "unreachable"
            
        // The 'hq' in ' hiveQuery { for v in hq do ... } gets evaluated here....
        | _ -> 
            let obj = Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation q 
            match obj with 
            | :? IHiveQueryable as hq -> hq.DataContext, hq.QueryData, hq.TailOps, hq.Timeout, (fun _ -> [])
            | _ -> failwithf "The following quotation evaluated to a '%s' instead of a HiveQueryable: %A" (obj.GetType().ToString()) q

    and decodeSelect nestedTableOpt canTail (hq,tableVar,auxVars,selectorBody) =
        let (hqCtxt:HiveDataContext), hqQueryData, hqTail, hqTimeout, hqAuxExprsF  = decodeQuery nestedTableOpt false hq

        // Substitute the aux expressions through the selection
        let hqAuxExprs = hqAuxExprsF tableVar
        let selectorBody = normalize auxVars hqAuxExprs selectorBody
        // when 'canTail' is true, 'select' can involve any expression at all, e.g. tuples. At the fringes we find expressions, e.g. "tableVar.column1 + tableVar.column2"
        let qQueryData, qTail = 
          match selectorBody with 
          | Var v when tableVar = v -> 
              hqQueryData, hqTail

          // Simple selections like 'select x.height' become 'SELECT *' followed by a tail op.
          | _ when 
                 // Only apply this optimiation for selections in 'tail' position which are actually
                 // about to produce in-memory objects.
                 canTail && 
                 // Check this is a simple selection.
                 isSimpleSelect hqQueryData && 
                 // We don't apply this optimization when there are nested queries in the result since they do not
                 // execute on the client side.
                 not (selectorBody |> existsSubQuotation (function NestedQuery(_) -> true | _ -> false)) -> 
              hqQueryData, hqTail @ [ Expr.Lambda(tableVar, selectorBody) ] 
          | _ -> 
            let tableName, colData = colDataOfQuery hqQueryData
            // First see if we are in the final ('tail') position where we can produce in-memory objects
            // using a tail expression.
            if canTail then 
                // If in tail position, extract the parts of the query that can be run on the server
                // side and replace them by variables. Then use the resulting expression as the tail expression.
                let recognizer = HiveExpressionTranslator(decodeQuery, hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=false)
                let next = let c = ref 0 in fun () -> incr c; string !c
                let newExtractor(t:Expr) = 
                    let extractorVar = new Var("Item"+next(),t.Type)
                    [(extractorVar,t)], Expr.Var(extractorVar)
                let rec extract t = 
                    if recognizer.RecognizeExpression t then 
                        newExtractor  t
                    else
                        match t with 
                        | Quotations.ExprShape.ShapeCombination(op,xs) -> 
                            let subProjs,subResidues = List.map extract xs  |> List.unzip
                            List.concat subProjs, ExprShape.RebuildShapeCombination(op,subResidues)
                        | Quotations.ExprShape.ShapeLambda(v,body) -> 
                            let subProjs,subResidue = extract body
                            subProjs, Expr.Lambda(v, subResidue)
                        | Quotations.ExprShape.ShapeVar _ -> [], t 
                let projs, residue = extract selectorBody
                let newTableVar = new Var("x",typeof<HiveDataRow>)
                
                // Replace all the extractor variables in the residue with lookups on the newTableVar
                let newProjection = 
                    (projs, residue) ||> List.foldBack (fun (extractorVar,_e) t -> 
                        let replacementForExtractionVar = 
                            let meth = HiveDataRow.GetValueMethodInfo.MakeGenericMethod(extractorVar.Type)
                            Expr.Call(Expr.Var(newTableVar),meth,[Expr.Value extractorVar.Name])
                        t.Substitute(fun v -> 
                            if v = extractorVar then Some replacementForExtractionVar
                            else None))  
                let newProjectionLambda = Expr.Lambda(newTableVar, newProjection)
                let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
                let colExprs = projs |> List.map (fun (v,e) -> (v.Name, converter.ConvertExpression e, isRequiredTy e.Type)) |> List.toArray
                let qQueryData = Select(hqQueryData, colExprs)
                qQueryData, hqTail @ [ newProjectionLambda ] 
            else
                // When a 'select' is not executable in-memory, it must be more restricted. For example
                //     ... select x.gender; ...
                //     ... select (x.gender, x.height) ...
                //     ... select {Gender=x.gender;Height=x.height} ....
                //     ... select (Data(Gender=x.gender,Height=x.height)) ...
                // In these cases, we can't execute the select as in-memory tail operations.
                let converter = HiveExpressionTranslator(decodeQuery,hqCtxt, tableName, tableVar, colData, auxVars, hqAuxExprs, allowEvaluation=true)
                let colExprs = 
                    match selectorBody with 
                    | Patterns.NewTuple(exprs) -> exprs |> List.mapi (fun i x -> "Item" + string (i+1), x) |> List.toArray
                    | Patterns.NewUnionCase(uc,exprs) -> (uc.GetFields(), exprs) ||> Seq.map2 (fun p x -> p.Name, x) |> Seq.toArray 
                    | Patterns.NewRecord(r,exprs) -> (FSharpType.GetRecordFields r, exprs) ||> Seq.map2 (fun p x -> p.Name, x) |> Seq.toArray 
                    | t -> [| ("Item1",t) |]
                let colExprs = colExprs |> Array.map (fun (nm,e) -> (nm,converter.ConvertExpression e, isRequiredTy e.Type))
                let qQueryData = Select(hqQueryData, colExprs)
                qQueryData, hqTail 


        hqCtxt, qQueryData, qTail, hqTimeout, (fun _ -> [])

    member __.For(source : HiveQueryable<'T>, selector : 'T -> 'U) : HiveQueryable<'U> = ignore (source,selector); failwith "The 'for' operator may only be executed on the server"

    [<CustomOperation("timeout", MaintainsVariableSpace = true)>]
    /// Specify a timeout for the query
    member __.Timeout(source : HiveQueryable<'T>, timeoutInSeconds : int) : HiveQueryable<'T> = ignore (source,timeoutInSeconds); failwith "The 'timeout' operator may only be executed on the server"

    [<CustomOperation("sampleBucket", MaintainsVariableSpace = true)>]
    /// Specify a sample bucket to use from a clsutered distributed table
    member __.SampleBucket(source : HiveQueryable<'T>, bucketNumber : int, numBuckets : int) : HiveQueryable<'T> = ignore (source,bucketNumber,numBuckets); failwith "The 'timeout' operator may only be executed on the server"

    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    /// Specify the maximum number of query element results to return
    member __.Take(source : HiveQueryable<'T>, n : int) : HiveQueryable<'T> = ignore (source,n); failwith "The 'take' operator may only be executed on the server"

    [<CustomOperation("count")>]
    /// Count the number of selected items
    member __.Count(source : HiveQueryable<'T>) : int64 = ignore (source); failwith "The 'count' operator may only be executed on the server"

    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    /// Restrict the selected items to those satisfying the given predicate
    member __.Where(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> bool) : HiveQueryable<'T> = ignore (source,selector); failwith "The 'where' operator may only be executed on the server"

    [<CustomOperation("select")>]
    /// Select the given items as the result of the query
    member __.Select(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'U> = ignore (source,selector); failwith "The 'select' operator may only be executed on the server"

    //[<CustomOperation("select1")>]
    //member __.Select1(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : 'U = ignore (source); failwith "The 'select1' operator may only be executed on the server"

    [<CustomOperation("averageBy")>]
    /// Evaluate the average of the given selection
    member inline __.AverageBy< 'T, ^Value when ^Value : (static member ( + ) : ^Value * ^Value -> ^Value) and  ^Value : (static member DivideByInt : ^Value * int -> ^Value) and  ^Value : (static member Zero : ^Value)>(source: HiveQueryable<'T>, [<ProjectionParameter>]_selector: 'T -> ^Value) : ^Value =  ignore (source); failwith "The 'averageBy' operator may only be executed on the server"

    [<CustomOperation("minBy")>]
    /// Evaluate the minimum of the given selection
    member __.MinBy<'T,'Key when 'Key : equality and 'Key : comparison> (source:HiveQueryable<'T>, [<ProjectionParameter>] valueSelector : 'T -> 'Key) : 'Key = ignore (source,valueSelector); failwith "The 'minBy' operator may only be executed on the server"

    [<CustomOperation("maxBy")>]
    /// Evaluate the maximum of the given selection
    member __.MaxBy<'T,'Key when 'Key : equality and 'Key : comparison> (source:HiveQueryable<'T>, [<ProjectionParameter>] valueSelector : 'T -> 'Key) : 'Key = ignore (source,valueSelector); failwith "The 'maxBy' operator may only be executed on the server"

    [<CustomOperation("sumBy")>]
    /// Evaluate the sum of the given selection
    member inline __.SumBy< 'T, ^Value when ^Value : (static member ( + ) : ^Value * ^Value -> ^Value) and  ^Value : (static member Zero : ^Value)>(source:HiveQueryable<'T>, [<ProjectionParameter>] valueSelector : ('T -> ^Value)) : ^Value =  ignore (source,valueSelector); failwith "The 'averageBy' operator may only be executed on the server"

    [<CustomOperation("averageByNullable")>]
    /// Evaluate the average of the given selection
    member inline __.AverageByNullable< 'T, ^Value when ^Value :> ValueType and ^Value : struct and ^Value : (new : unit -> ^Value) and ^Value : (static member ( + ) : ^Value * ^Value -> ^Value) and  ^Value : (static member DivideByInt : ^Value * int -> ^Value) and  ^Value : (static member Zero : ^Value)  >(source: HiveQueryable<'T>, [<ProjectionParameter>]_selector: 'T -> Nullable< ^Value >) : Nullable< ^Value >   =  ignore (source); failwith "The 'averageByNullable' operator may only be executed on the server"

    [<CustomOperation("minByNullable")>]
    /// Evaluate the minimum of the given selection
    member __.MinByNullable<'T,'Key when 'Key : equality and 'Key : comparison and 'Key : (new : unit -> 'Key) and 'Key : struct and 'Key :> ValueType> (source:HiveQueryable<'T>, [<ProjectionParameter>]valueSelector : 'T -> Nullable<'Key>) : Nullable<'Key> =  ignore (source,valueSelector); failwith "The 'maxBy' operator may only be executed on the server"

    [<CustomOperation("maxByNullable")>]
    /// Evaluate the maximum of the given selection
    member __.MaxByNullable<'T,'Key when 'Key : equality and 'Key : comparison and 'Key : (new : unit -> 'Key) and 'Key : struct and 'Key :> ValueType> (source:HiveQueryable<'T>, [<ProjectionParameter>]valueSelector : 'T -> Nullable<'Key>) : Nullable<'Key> = ignore (source,valueSelector); failwith "The 'maxBy' operator may only be executed on the server"

    [<CustomOperation("sumByNullable")>]
    /// Evaluate the sum of the given selection
    member inline __.SumByNullable<'T, ^Value when ^Value :> ValueType and ^Value : struct  and ^Value : (new : unit -> ^Value)  and ^Value : (static member ( + ) : ^Value * ^Value -> ^Value)  and  ^Value : (static member Zero : ^Value)>(source: HiveQueryable<'T>, [<ProjectionParameter>]valueSelector : 'T -> Nullable< ^Value >) : Nullable< ^Value >  = ignore (source,valueSelector); failwith "The 'sumByNullable' operator may only be executed on the server"

#if GROUP_BY
   // TODO: THis feature is close to working, for example:
   //
   // let query8 = hiveQuery {for line in context.sample_08 do
   //                         where (line.total_emp ?> 1500) 
   //                         groupBy line.total_emp into g
   //                         select (g.Key,hiveQuery { for x in g do averageBy (float x.salary) })  }
   //
   // The problem is that the "groupBy" gets translated as "SELECT *, {group-selection-expr} FROM ... GROUP BY {key-expr}"
   //
   // The "*" here is intended to select the key - however it selects a subset of the columns and/or other expressions. This causes a mismatch between
   // the expected and actual number and order of columns in the query.
   //

    [<CustomOperation("groupBy",AllowIntoPattern=true)>] 
    /// Group the selection by the given key
    member __.GroupBy<'T,'Key when 'Key : equality> (source: HiveQueryable<'T>, [<ProjectionParameter>] keySelector:('T -> 'Key)) : HiveQueryable<HiveGrouping<'Key,'T>>  = ignore (source,keySelector); failwith "The 'groupBy' operator may only be executed on the server"

    //member __.GroupValBy<'T,'Key,'Result, 'Q when 'Key : equality > (source:HiveQueryable<'T>, resultSelector: 'T -> 'Result, keySelector: 'T -> 'Key) : HiveQueryable<System.Linq.IGrouping<'Key,'Result>,'Q>   = 

#endif

    [<CustomOperation("distinct",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    /// Ensure the elements of the selection are distinct
    member __.Distinct<'T when 'T : equality>(source:HiveQueryable<'T>) : HiveQueryable<'T>  = ignore (source); failwith "The 'distinct' operator may only be executed on the server"

#if SORT_AND_JOIN_ON_HIVE_TABLES // NYI
    //member __.SortBy (source: HiveQueryable<'T>, keySelector : 'T -> 'Key) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.SortByDescending (source: HiveQueryable<'T>, keySelector : 'T -> 'Key) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.ThenBy (source: HiveQueryable<'T>, keySelector : 'T -> 'Key) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison = 
    //member __.ThenByDescending (source: HiveQueryable<'T>, keySelector : 'T -> 'Key) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.SortByNullable (source: HiveQueryable<'T>, keySelector : 'T -> Nullable<'Key>) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.SortByNullableDescending (source: HiveQueryable<'T>, keySelector : 'T -> Nullable<'Key>) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.ThenByNullable (source: HiveQueryable<'T>, keySelector : 'T -> Nullable<'Key>) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison = 
    //member __.ThenByNullableDescending (source:  HiveQueryable<'T>, keySelector : 'T -> Nullable<'Key>) : HiveQueryable<'T> when 'Key : equality and 'Key : comparison  = 
    //member __.Join  (outerSource: HiveQueryable<_,'Q>, innerSource: HiveQueryable<_,'Q>, outerKeySelector, innerKeySelector, elementSelector) : HiveQueryable<_,'Q> = 
    //member __.GroupJoin (outerSource: HiveQueryable<_,'Q>, innerSource: HiveQueryable<_,'Q>, outerKeySelector, innerKeySelector, elementSelector: _ ->  seq<_> -> _) : HiveQueryable<_,'Q> = 
    //member __.LeftOuterJoin (outerSource:HiveQueryable<_,'Q>, innerSource: HiveQueryable<_,'Q>, outerKeySelector, innerKeySelector, elementSelector: _ ->  seq<_> -> _) : HiveQueryable<_,'Q> = 
#endif

    [<CustomOperation("writeRows", MaintainsVariableSpace = true)>]
    /// Write to an existing table, overwriting any previous rows in the table. Use table.NewRow to specify a row.
    member __.WriteRows(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'T> = ignore (source,selector); failwith "The 'writeRows' operator may only be executed on the server"


    [<CustomOperation("writePartition", MaintainsVariableSpace = true)>]
    /// Write to an existing partition, overwriting any previous rows in the partition. Use table.NewRow to specify a row. The partition keys must be not depend on the selection context.
    member __.WritePartition(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'T> = ignore (source,selector); failwith "The 'writePartition' operator may only be executed on the server"

    [<CustomOperation("insertRows", MaintainsVariableSpace = true)>]
    /// Insert new rows into an existing table.  Use table.NewRow to specify a row.
    member __.InsertRows(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'T> = ignore (source,selector); failwith "The 'insertRows' operator may only be executed on the server"

    [<CustomOperation("insertPartition", MaintainsVariableSpace = true)>]
    /// Insert new rows into an existing partition.  Use table.NewRow to specify a row. The partition keys must be not depend on the selection context.
    member __.InsertPartition(source : HiveQueryable<'T>, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'T> = ignore (source,selector); failwith "The 'writePartition' operator may only be executed on the server"


    [<CustomOperation("newTable")>]
    /// Write new rows to a new intermediate table. Any previous table with this name is dropped and overwritten.
    member __.NewTable(source : HiveQueryable<'T>, tableName: string, [<ProjectionParameter>]selector : 'T -> 'U) : HiveTable<'U> = ignore (source, tableName, selector); failwith "The 'newTable' operator may only be executed on the server"

    // Taken this out for now - the file doesn't actually seem to get written correctly.
    //
    // [<CustomOperation("writeHeadNodeFile")>]
    // /// Write the new rows to a local file
    // member __.WriteHeadNodeFile(source : HiveQueryable<'T>, fileName: string, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'U> = ignore (source); failwith "The 'writeHeadNodeFile' operator may only be executed on the server"

    [<CustomOperation("writeDistributedFile", MaintainsVariableSpace = true)>]
    /// Write the selection to a distributed file
    member __.WriteDistributedFile(source : HiveQueryable<'T>, fileName: string, [<ProjectionParameter>]selector : 'T -> 'U) : HiveQueryable<'T> = ignore (source,fileName,selector); failwith "The 'writeHeadNodeFile' operator may only be executed on the server"

    member __.Yield(_x:'T) : 'T = failwith "The 'yield' operator may only be executed on the server"
    member __.Quote() = ()
 
    // This is the primary entry point to the query implementation. The value 'q' is the desugared
    // quotation for the contents of hiveQuery { ... }. We must use Hive to 'eval' this quotation.
    member sp.Run(q:Expr<'T>) : 'T =  
        
        // First, analyze the quotation and extract the query.
        let qCtxt, qQueryData, qTailOps, qTimeout, _qAuxExprs = decodeQuery None true q

        let resTy = typeof<'T> 
        // Is the result of the query a HiveTable<_>? If so, create the table object. We have
        // to use reflection to do this to set the right value for the type parameter.
        if resTy.IsGenericType && resTy.GetGenericTypeDefinition() = typedefof<HiveTable<_>> && (match qQueryData with Table _ -> true | _ -> false ) then 
            // wrap it up - 'T is HiveTable<_>
            let tableName, colData = colDataOfQuery qQueryData
            resTy.GetConstructors(BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance).[0].Invoke([|qCtxt; tableName; colData|]) |> box |> unbox 

        // Is the result of the query a HiveQueryable<_>? If so, create the queryable object. 
        elif resTy.IsGenericType && resTy.GetGenericTypeDefinition() = typedefof<HiveQueryable<_>> then 
            // wrap it up - 'T is HiveQueryable<_>
            resTy.GetConstructors(BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance).[0].Invoke([|qCtxt; qQueryData; qTailOps; (None: int option)|]) |> box |> unbox 

        // Otherwise, this is th result of an aggregation operator and we execute the query immediately
        else 
            qCtxt.ExecuteQueryValue(qQueryData, ?timeout=qTimeout) |> unbox


                                                                        
[<TypeProvider>]
type HiveTypeProviderImplementation(_config: TypeProviderConfig) as this = 
    inherit TypeProviderForNamespaces()

    let ns = "Hive"

    let asm = Assembly.GetExecutingAssembly()
    
    let memoizedFetcher paras req = HiveRequestInProcess paras req
    let memoizedGetTables = memoize (fun param -> memoizedFetcher(param)(GetTableNames))
    let memoizedGetTableDescription = memoize (fun (param,tableName) -> memoizedFetcher(param)(GetTableDescription(tableName)))
    let memoizedGetTableSchema = memoize (fun (param,tableName) -> memoizedFetcher(param)(GetTableSchema(tableName)))

    let hiveTy = 
        let hiveTyped = ProvidedTypeDefinition(asm, ns, "HiveTypeProvider", Some(typeof<obj>), HideObjectMethods = true)
        let dsnArg = ProvidedStaticParameter("DSN", typeof<string>)
        let reqArg   = ProvidedStaticParameter("UseRequiredAnnotations", typeof<bool>,true)
        let queryTimeoutArg  = ProvidedStaticParameter("DefaultQueryTimeout", typeof<int>, 60)
        let metadataTimeoutArg  = ProvidedStaticParameter("DefaultMetadataTimeout", typeof<int>, 5)
        let helpText = "<summary>Typed representation of the Hive Tables</summary>
                        <param name='DSN'>The ODBC DSN</param>
                        <param name='UseRequiredAnnotations'>Use 'required' annotations in Hive table descriptions to give annotated F# types (default: true). The annotation '(required)' must be at the end of the table description.</param>
                        <param name='DefaultQueryTimeout'>The timeout in seconds used when fetching metadata from the Hive ODBC service at compile-time, and the default timeout for accesses to the Hive ODBC service at runtime (default: 60 seconds).</param>
                        <param name='DefaultMetadataTimeout'>The timeout in seconds used when fetching metadata from the Hive ODBC service at compile-time, and the default timeout for accesses to the Hive ODBC service at runtime (default: 5 seconds).</param>"

        do hiveTyped.AddXmlDoc(helpText)
        do hiveTyped.DefineStaticParameters([dsnArg; reqArg;queryTimeoutArg;metadataTimeoutArg], fun typeName providerArgs ->
                let dsn, useRequiredAnnotations,queryTimeout,metadataTimeout = 
                    match providerArgs with 
                    | [| :? string as dsn;
                         :? bool as useRequiredAnnotations; 
                         :? int as queryTimeout; 
                         :? int as metadataTimeout
                      |]  -> 
                        (dsn, useRequiredAnnotations,queryTimeout,metadataTimeout)
                    | args -> 
                        failwithf "unexpected arguments to type provider, got %A" args
                let queryTimeout = queryTimeout * 1<s>
                let metadataTimeout = metadataTimeout * 1<s>
                if String.IsNullOrWhiteSpace dsn then failwith "DSN is required" 
                let param = (dsn,metadataTimeout)

                let root = ProvidedTypeDefinition(asm, ns, typeName, baseType = Some typeof<obj>, HideObjectMethods = true) 
                let containerForRowTypes = ProvidedTypeDefinition("DataTypes", baseType = Some typeof<obj>, HideObjectMethods = true) 
                root.AddMember(containerForRowTypes)
                let dataServiceType = ProvidedTypeDefinition("DataService", baseType = Some typeof<obj>, HideObjectMethods = true) 
                containerForRowTypes.AddMember(dataServiceType)
                dataServiceType.AddXmlDoc "Represents the connection configuration for accessinng a Hadoop/Hive data store"
                dataServiceType.AddMembersDelayed (fun () -> 
                    let tableNames = 
                        memoizedGetTables param |> function 
                            | TableNames(names) -> names
                            | data -> RuntimeHelpers.unexpected "TableNames" data
                    let rowTypes, tableTypes = 
                        [ for tableName in tableNames do
                            let rowType = ProvidedTypeDefinition(tableName, Some typeof<HiveDataRow>, HideObjectMethods = true) 
                            let tableColumnInfoLazy = 
                              lazy 

                                let tableInfo = 
                                    match memoizedGetTableSchema(param,tableName) with
                                    | TableSchema(tableInfo) -> tableInfo
                                    | data -> RuntimeHelpers.unexpected "TableDescription" data

                                let fsharpColumnTypes = 
                                    [ for col in tableInfo.Columns do 
                                        let fsharpColumnTypeWithoutNullable = computeFSharpTypeWithoutNullable(col.HiveType)
                                        let fsharpColumnType = 
                                            // 'string' and other reference types are already nullable in .NET land
                                            if (useRequiredAnnotations && col.IsRequired) || not fsharpColumnTypeWithoutNullable.IsValueType then 
                                                fsharpColumnTypeWithoutNullable
                                            else
                                                ProvidedTypeBuilder.MakeGenericType(typedefof<Nullable<_>>, [ fsharpColumnTypeWithoutNullable ])
                                        yield  fsharpColumnType ]
                                (tableInfo, fsharpColumnTypes)

                            rowType.AddMembersDelayed (fun () -> 
                                let props = 
                                    let tableInfo, fsharpColumnTypes = tableColumnInfoLazy.Force()
                                    [ for col, fsharpColumnType in Seq.zip tableInfo.Columns fsharpColumnTypes do 
                                        let p = 
                                            ProvidedProperty
                                                (propertyName = col.HiveName, propertyType = fsharpColumnType, IsStatic=false, 
                                                 GetterCode= (fun args -> 
                                                    let meth = HiveDataRow.GetValueMethodInfo
                                                    let meth2 = ProvidedTypeBuilder.MakeGenericMethod(meth, [ fsharpColumnType ])
                                                    let hqQueryData = Expr.Call(args.[0], meth2, [Expr.Value(col.HiveName)])
                                                    hqQueryData))
                                        let colDesc = 
                                            if String.IsNullOrWhiteSpace col.Description || col.Description = "null"  then 
                                                "The column " + col.HiveName + " in the table " + tableName + " in the Hive metastore." 
                                            else col.Description + "."
                                        let isPartitionKey = tableInfo.PartitionKeys |> Array.exists (fun x -> x = col.HiveName)
                                        //let isBucketKey = tableInfo.BucketKeys |> Array.exists (fun x -> x = col.HiveName)
                                        //let isSortKey = tableInfo.SortKeys |> Array.exists (fun x -> x.Column = col.HiveName)
                                        let colDesc = colDesc + (if isPartitionKey then " The column is a partition key." else "")
                                        //let colDesc = colDesc + (if isBucketKey then " The column is a bucket key." else "")
                                        //let colDesc = colDesc + (if isSortKey then " The column is a sort key." else "")
                                        p.AddXmlDoc colDesc
                                        yield p  ]
                                props)
                            let tableBaseType = typedefof<HiveTable<_>>.MakeGenericType [| (rowType :> System.Type) |]
                            let tableType = ProvidedTypeDefinition(tableName+"Table", Some tableBaseType, HideObjectMethods = true) 

                            tableType.AddMembersDelayed (fun () -> 
                                let tableInfo, exactColumnTypes = tableColumnInfoLazy.Force()
                                let ctorArgs = 
                                    [ for col, exactColumnType in Seq.zip tableInfo.Columns exactColumnTypes do 
                                       yield ProvidedParameter(parameterName = col.HiveName, parameterType = exactColumnType,?optionalValue= (if col.IsRequired then None else Some null)) ]
                                let ctor = 
                                    ProvidedMethod("NewRow",
                                                   ctorArgs,
                                                   rowType,
                                                   IsStaticMethod=false,
                                                   InvokeCode= (fun args -> 
                                                        let hqQueryData = Expr.NewObject(HiveDataRow.HiveDataRowCtor, 
                                                                                         [Expr.Value tableName;
                                                                                          Expr.NewArray(typeof<string * obj * bool>,
                                                                                                       [ for x,(arg,col) in (Seq.zip ctorArgs (Seq.zip (List.tail args) tableInfo.Columns)) -> 
                                                                                                           let isPartitionKey = tableInfo.PartitionKeys |> Array.exists (fun x -> x = col.HiveName)
                                                                                                           Expr.NewTuple [Expr.Value x.Name; Expr.Coerce(arg,typeof<obj>); Expr.Value isPartitionKey]])])
                                                        // TODO: bind the XML doc for the various parameters
                                                        hqQueryData))
                                [ctor])
                            yield rowType,(tableType,tableColumnInfoLazy) ]
                      |> List.unzip

                    containerForRowTypes.AddMembers(List.map fst tableTypes)
                    containerForRowTypes.AddMembers(rowTypes)

                    let dcProp = ProvidedProperty(propertyName = "DataContext", propertyType = typeof<HiveDataContext>, GetterCode= (fun args -> Expr.Coerce(args.[0], typeof<HiveDataContext>))) 
                    do dcProp.AddXmlDocDelayed(fun () -> "The underlying data context for dynamic operations")
                    
                    let tableProps = 
                        [ for (tableName,(tableType,tableColumnInfoLazy)) in Seq.zip tableNames tableTypes do
                                let p = ProvidedProperty(propertyName = tableName, propertyType = tableType, 
                                                        GetterCode= (fun args -> <@@ (%%(Expr.Coerce(args.[0], typeof<HiveDataContext>)) : HiveDataContext).GetTable<HiveDataRow>(tableName) @@>))
                                p.AddXmlDocDelayed(fun () -> 
                                    let tableInfo, _ = tableColumnInfoLazy.Force()
                                    let tableDesc,tableHead = 
                                        match memoizedGetTableDescription(param, tableName) with
                                        | TableDescription(tableDesc,tableHead) -> tableDesc,tableHead
                                        | Exception ex -> sprintf "table '%s'" tableName, sprintf "error %s" ex
                                        | data -> sprintf "table '%s'" tableName, sprintf "unexpected result %A" data

                                    let tablePartitionKeys = 
                                        match tableInfo.PartitionKeys with 
                                        | [| |] -> "No partition keys."
                                        | keys -> "Partition keys: " + String.concat ", " keys + "."
(*
                                    let tableBucketKeys = 
                                        match tableInfo.BucketKeys with 
                                        | [| |] -> "No bucket keys."
                                        | keys -> "Bucket keys: " + String.concat ", " keys + "."
                                    let tableSortKeys = 
                                        match tableInfo.SortKeys with 
                                        | [| |] -> "No sort keys."
                                        | keys -> "Sort keys: " + String.concat ", " (keys |> Array.map (fun x -> x.Column)) + "."
 *)                                   
                                    if tableDesc = "" 
                                    then tableName 
                                    else "todo - Visual Studio Docs"
                                    )

                                yield p ]
                    dcProp :: tableProps)

                let getDataContextMeth = 
                    ProvidedMethod("GetDataContext",
                                   [ 
                                     
                                     ProvidedParameter("dsn",typeof<string>,optionalValue=dsn);
                                     ProvidedParameter("queryTimeout",typeof<int>,optionalValue=queryTimeout);
                                     ProvidedParameter("metadataTimeout",typeof<int>,optionalValue=metadataTimeout)    ],
                                    dataServiceType,
                                    InvokeCode= (fun args -> <@@ new HiveDataContext((%%args.[0] : string),(%%args.[1] : int<s>),(%%args.[2] : int<s>)) @@>),
                                    IsStaticMethod=true)
                root.AddMembers [getDataContextMeth ]
                        
                root)
        hiveTyped
    do this.AddNamespace(ns, [hiveTy])

[<assembly:TypeProviderAssembly>] 
do()

[<AutoOpen>]
module  HiveQueryBuilderStatics = 
    let hiveQuery = HiveQueryBuilder()