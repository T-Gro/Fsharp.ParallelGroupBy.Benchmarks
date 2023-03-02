open System
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open BenchmarkDotNet.Diagnosers


module SequentialImplementation = 
    let groupBy = Array.groupBy

module PLINQImplementation = 
    let groupBy (projection) (array:'T array) = 
        let projection = Func<_,_>(projection)
        array.AsParallel().GroupBy(projection).Select(fun x -> (x.Key,x.ToArray())).ToArray()

module CustomImpl =
    // The following two parameters were benchmarked and found to be optimal.
    // Benchmark was run using: 11th Gen Intel Core i9-11950H 2.60GHz, 1 CPU, 16 logical and 8 physical cores
    let private maxPartitions = Environment.ProcessorCount // The maximum number of partitions to use
    let private sequentialCutoffForGrouping = 2_500 // Arrays smaller then this will be sorted sequentially
    let private minChunkSize = 64 // The minimum size of a chunk to be sorted in parallel

    let private createPartitions (array: 'T[]) =
        [|
            let chunkSize =
                match array.Length with
                | smallSize when smallSize < minChunkSize -> smallSize
                | biggerSize when biggerSize % maxPartitions = 0 -> biggerSize / maxPartitions
                | biggerSize -> (biggerSize / maxPartitions) + 1

            let mutable offset = 0

            while (offset + chunkSize) <= array.Length do
                yield new ArraySegment<'T>(array, offset, chunkSize)
                offset <- offset + chunkSize

            if (offset <> array.Length) then
                yield new ArraySegment<'T>(array, offset, array.Length - offset)
        |]

    let atLeastProjectionInParallel projection array =     
        array 
        |> Array.Parallel.map (fun x -> struct(projection x,x))
        |> Array.groupBy (fun struct(key,value) -> key) 
        |> Array.Parallel.map (fun (key,group) -> (key,group |> Array.map (fun struct(key,value) -> value)))

    let concurrentMultiDictionairy projection array = 
        let dict = new ConcurrentDictionary<_,_>()
        let valueFactory = Func<_,_>(fun _ -> new ConcurrentBag<_>())
        array |> Array.Parallel.iter (fun x -> 
            let key = projection x
            let bucket = dict.GetOrAdd(key,valueFactory=valueFactory)
            bucket.Add(x))

        dict.ToArray() |> Array.Parallel.map (fun kvp -> (kvp.Key,kvp.Value.ToArray()))

    let countByThenAssign projection array =       
        let counts = new ConcurrentDictionary<_,_>(concurrencyLevel = maxPartitions, capacity = 20*2)
        let valueFactory = new Func<_,_>(fun _ -> ref 0)
        array |> Array.Parallel.iter (fun item ->             
                    let counter = counts.GetOrAdd(projection item,valueFactory=valueFactory)
                    System.Threading.Interlocked.Increment(counter) |> ignore)

        let sortedCounts = counts.ToArray()
        let finalResults = sortedCounts |> Array.Parallel.map (fun kvp -> (kvp.Key,Array.zeroCreate kvp.Value.Value))
        let finalresultsLookup = finalResults |> dict
        array |> Array.Parallel.iter (fun value ->
            let key = projection value
            let correctBucket = finalresultsLookup[key]
            let idxToWrite = System.Threading.Interlocked.Decrement(counts[key])
            correctBucket.[idxToWrite] <- value
            )
        finalResults

    let eachChunkSeparatelyThenMerge projection array =     

        let partitions = createPartitions array
        let resultsPerChunk = Array.init partitions.Length (fun _ -> new Dictionary<_,ResizeArray<_>>()) // MAX. one entry per final key
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = resultsPerChunk[i]
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, bucket -> bucket.Add(x)
                | false, _ -> 
                    let newList = new ResizeArray<_>(1)  
                    newList.Add(x)
                    localDict.Add(key,newList)           
        )

        // O ( threads * groups)
        let allResults = new Dictionary<_,ResizeArray<ResizeArray<_>>>()  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, bucket -> bucket.Add(kvp.Value)
                | false, _ -> 
                    let newBuck = new ResizeArray<_>(partitions.Length) // MAX. one per partition, in theory less
                    newBuck.Add(kvp.Value)
                    allResults.Add(kvp.Key,newBuck)

        let results = Array.zeroCreate allResults.Count
        let mutable partitionIdx = 0
        // O(N)
        for kvp in allResults do
            let key = kvp.Key
            let values = kvp.Value
            let finalArrayForKey = Array.zeroCreate (values |> Seq.sumBy (fun x -> x.Count))
            let mutable finalArrayOffset = 0
            for bucket in values do
                bucket.CopyTo(finalArrayForKey,finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + bucket.Count
           
            results.[partitionIdx] <- (key,finalArrayForKey)
            partitionIdx <- partitionIdx + 1
        results

    let eachChunkSeparatelyViaList projection array =     

        let partitions = createPartitions array
        let resultsPerChunk = Array.init partitions.Length (fun _ -> new Dictionary<_,Ref<list<_>>>()) // MAX. one entry per final key
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = resultsPerChunk[i]
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, list ->                   
                    list.Value <- x :: list.Value
                | false, _ ->                
                    localDict.Add(key,ref [x])                 
        )

        // O ( threads * groups)
        let resultsPerKey = new Dictionary<_,_>(resultsPerChunk[0].Count * 2)  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match resultsPerKey.TryGetValue(kvp.Key) with
                | false, _ -> resultsPerKey.Add(kvp.Key,kvp.Value) 
                | true, list -> list.Value <- kvp.Value.Value @ list.Value
                

        let results = Array.zeroCreate resultsPerKey.Count
        let nonFlattenResults = resultsPerKey.ToArray()

        Parallel.For(0,nonFlattenResults.Length, fun i ->            
            let kvp = nonFlattenResults.[i] 
            results.[i] <- (kvp.Key,kvp.Value.Value |> List.toArray)
        )
      
        results

      

type IBenchMarkElement<'T when 'T :> IBenchMarkElement<'T>> =
    static abstract Create: int * float -> 'T
    static abstract Projection: unit -> ('T -> int)


type ReferenceRecord = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecord> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Id.ToString().GetHashCode() * (x.Value |> sin |> string |> hash)) % 20

type ReferenceRecordManyBuckets = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecordManyBuckets> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Value.GetHashCode() * x.Id.GetHashCode() ) % 10_000

[<Struct>]
type StructRecord = {Id : int; Value : float}
    with interface IBenchMarkElement<StructRecord> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> x.Id % 20


[<MemoryDiagnoser>]
[<ThreadingDiagnoser>]
[<GenericTypeArguments(typeof<ReferenceRecord>)>]
[<GenericTypeArguments(typeof<ReferenceRecordManyBuckets>)>]
[<GenericTypeArguments(typeof<StructRecord>)>]
//[<DryJob>]  // Uncomment heere for quick local testing
type ArrayParallelGroupByBenchMark<'T when 'T :> IBenchMarkElement<'T>>() = 

    let r = new Random(42)

    [<Params(500,4_000,100_000,2_500_000)>]      
    member val NumberOfItems = -1 with get,set

    member val ArrayWithItems = Unchecked.defaultof<'T[]> with get,set

    [<GlobalSetup>]
    member this.GlobalSetup () = 
        this.ArrayWithItems <- Array.init this.NumberOfItems (fun idx -> 'T.Create(idx,r.NextDouble()))        

    [<Benchmark(Baseline = true)>]
    member this.Sequential () = 
        this.ArrayWithItems |> SequentialImplementation.groupBy ('T.Projection())

    [<Benchmark>]
    member this.PLINQDefault () = 
        this.ArrayWithItems |> PLINQImplementation.groupBy ('T.Projection())

    [<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMerge ('T.Projection())

    [<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyViaList ('T.Projection())

    //[<Benchmark>]
    member this.CountByThenAssign () = 
        this.ArrayWithItems |> CustomImpl.countByThenAssign ('T.Projection())

    //[<Benchmark>]
    member this.AtLestProjectionInParallel () = 
        this.ArrayWithItems |> CustomImpl.atLeastProjectionInParallel ('T.Projection())

    //[<Benchmark>]
    member this.ConcurrentDictOfBags () = 
        this.ArrayWithItems |> CustomImpl.concurrentMultiDictionairy ('T.Projection())



[<EntryPoint>]
let main argv =
    BenchmarkSwitcher.FromTypes([|typedefof<ArrayParallelGroupByBenchMark<ReferenceRecord> >|]).Run(argv) |> ignore   
    0