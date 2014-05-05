#r "System.Xml.Linq"
#r "System.IO.Compression"

open System
open System.IO
open System.Net
open System.Windows
open System.Windows.Input
open System.Xml.Linq
open System.Xml

module Zip =     
    open System.IO.Compression
    let createArchive(entries:(string*byte[])[]) =    
        use ms = new MemoryStream()
        let zipArchive = new ZipArchive(ms,ZipArchiveMode.Create)
        for (name,data) in entries do
            let entry = zipArchive.CreateEntry(name)
            use str = entry.Open()
            str.Write(data,0,data.Length)
            str.Close()
        zipArchive.Dispose()
        ms.ToArray()        


module String =
    let toBytes(str:string) = System.Text.Encoding.UTF8.GetBytes(str)
    let ofBytes(bytes:byte[]) = System.Text.Encoding.UTF8.GetString(bytes)
    let ofXNode(x:XNode) = x.ToString(SaveOptions.OmitDuplicateNamespaces)

let nugetApiKey = File.ReadAllText(__SOURCE_DIRECTORY__ + "\\nugetApiKey")

let postNuget(filename:string) (data:byte[]) =
    let req = HttpWebRequest.Create("https://www.nuget.org/api/v2/package")
    req.Method <- "PUT"
    let boundary = "---------------------------" + DateTime.Now.Ticks.ToString("x");
    let boundarybytes = System.Text.Encoding.ASCII.GetBytes("\r\n--" + boundary + "\r\n");
    req.ContentType <- "multipart/form-data; boundary=" + boundary
    req.Headers.["X-NuGet-ApiKey"] <- nugetApiKey
    let rs = req.GetRequestStream()
    let toBytes (str:string) = System.Text.Encoding.ASCII.GetBytes(str) 
    let data = 
        [|
            sprintf "--%s\r\n" boundary |> toBytes
            sprintf "Content-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\n" filename filename |> toBytes
            "Content-Type: application/octet-stream\r\n\r\n" |> toBytes
            data
            "\r\n" |> toBytes
            sprintf "--%s--" boundary |> toBytes
        |] |> Array.collect id
    rs.Write(data,0,data.Length)
    rs.Close()
    let resp = req.GetResponse()
    let respStream = resp.GetResponseStream()
    use ms = new MemoryStream()
    respStream.CopyTo(ms)
    System.Text.Encoding.ASCII.GetString(ms.ToArray())


let name = "HiveTypeProvider"
let version = "1.0.0.2"

let nugetXml = 
    let nuget = XNamespace.Get "http://schemas.microsoft.com/packaging/2011/08/nuspec.xsd"
    let xmlHead = """<?xml version="1.0" encoding="utf-8"?>"""
    XElement(nuget + "package",
        XElement(nuget + "metadata",
            [|
                "id", name
                "version", version
                "title","HiveTypeProvider"
                "authors","moloneymb"
                "owners", "moloneymb"
                //"licenseUrl", "https://lz4net.codeplex.com/license"
                //"projectUrl", "https://lz4net.codeplex.com/"
                "requireLicenseAcceptance", "false"
                "description", "Interactivly query your Hive database"
                "summary", "Interactivly query your Hive database"
                "releaseNotes", "Initial Upload"
                "copyright", "moloneymb"
                "tags", "fsharp typeprovider bigdata hive hadoop"
            |] |> Array.map (fun (name,value) -> XElement(nuget + name, value))
            )
        ) |> String.ofXNode |> sprintf "%s\n%s" xmlHead

[|
    yield name + ".nuspec", nugetXml |> String.toBytes 
    for ext in [|".dll";".pdb";".XML"|] -> sprintf "lib/net40/%s%s" name ext, File.ReadAllBytes(__SOURCE_DIRECTORY__ + sprintf "\\bin\\Debug\\%s%s" name ext)
|] 
|> Zip.createArchive
|> postNuget (name + version)

