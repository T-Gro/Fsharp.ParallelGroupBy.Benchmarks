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
    let sortBy = Array.sortBy

module PLINQImplementation = 
    let groupBy (projection) (array:'T array) = 
        let projection = Func<_,_>(projection)
        array.AsParallel().GroupBy(projection).Select(fun x -> (x.Key,x.ToArray())).ToArray()

    let sortBy (projection) (array:'T array) = 
        let projection = Func<_,_>(projection)
        array.AsParallel().OrderBy(projection).ToArray()

module CustomImpl =

    let inline groupByInPlaceViaSort ([<InlineIfLambda>]projection) array = 
        let projectedFields = Sorting.sortInPlaceBy projection array      
        let segments = new ResizeArray<_>(1024)
        let mutable lastKey = projectedFields.[0]
        let mutable lastKeyIndex = 0
        for i=1 to array.Length-1 do
            let currentKey = projectedFields.[i]
            if currentKey <> lastKey then
                segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, i - lastKeyIndex))
                lastKey <- currentKey
                lastKeyIndex <- i

        segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, array.Length - lastKeyIndex))
        segments.ToArray()

    let inline groupByInPlaceViaSortAndParallelSegmentation ([<InlineIfLambda>]projection) array = 
        let projectedFields = Sorting.sortInPlaceBy projection array      

        let partitions = Shared.createPartitions array
        let possiblyOverlappingSegments = Array.zeroCreate partitions.Length

        Parallel.For(0, partitions.Length, fun i ->
            let segments = new ResizeArray<_>()
            let partition = partitions[i]
            let mutable lastKey = projectedFields.[partition.Offset]
            let mutable lastKeyIndex = partition.Offset
            for i=(partition.Offset+1) to (partition.Offset + partition.Count - 1) do
                let currentKey = projectedFields.[i]
                if currentKey <> lastKey then
                    segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, i - lastKeyIndex))
                    lastKey <- currentKey
                    lastKeyIndex <- i  

            segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, array.Length - lastKeyIndex))
            possiblyOverlappingSegments[i] <- segments
        ) |> ignore

        let allSegments = possiblyOverlappingSegments[0]
        allSegments.EnsureCapacity(possiblyOverlappingSegments |> Array.sumBy (fun ra -> ra.Count)) |> ignore
        for i=1 to (possiblyOverlappingSegments.Length-1) do
            let currentSegment = possiblyOverlappingSegments[i]
            let (prevLastKey,prevLastSegment) = allSegments[allSegments.Count]
            let (thisFirstKey,thisFirstSegment) = currentSegment[0]

            if prevLastKey <> thisFirstKey then
                allSegments.AddRange(currentSegment)
            else
                allSegments[allSegments.Count] <- prevLastKey,new ArraySegment<_>(array,prevLastSegment.Offset, prevLastSegment.Count + thisFirstSegment.Count)
                allSegments.AddRange(currentSegment.Skip(1))

        allSegments.ToArray()
       

    let inline sortThenCreateGroups ([<InlineIfLambda>]projection) array = 
        let sorted = Array.sortBy projection array
        let segments = new ResizeArray<_>(1024)
        let mutable lastKey = projection sorted.[0]
        let mutable lastKeyIndex = 0
        for i=1 to sorted.Length-1 do
            let currentKey = projection sorted.[i]
            if currentKey <> lastKey then
                segments.Add(lastKey,new ArraySegment<'T>(sorted,lastKeyIndex, i - lastKeyIndex))
                lastKey <- currentKey
                lastKeyIndex <- i

        segments.Add(lastKey,new ArraySegment<'T>(sorted,lastKeyIndex, sorted.Length - lastKeyIndex))
        segments.ToArray()

    let inline sortThenCreateGroupsToArray ([<InlineIfLambda>]projection) array =
        let g = sortThenCreateGroups projection array
        g |> Array.Parallel.map (fun (key,segment) -> (key,segment.ToArray()))

    let eachChunkSeparatelyThenMerge projection array =     

        let partitions = Shared.createPartitions array
        let resultsPerChunk = Array.zeroCreate partitions.Length
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = new Dictionary<_,ResizeArray<_>>(partitions.Length)
            resultsPerChunk[i] <- localDict
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, bucket -> bucket.Add(x)
                | false, _ -> 
                    let newList = new ResizeArray<_>(1)  
                    newList.Add(x)
                    localDict.Add(key,newList)) |> ignore           


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
        let nonFlattenResults = allResults.ToArray()

        Parallel.For(0,nonFlattenResults.Length, fun i ->            
            let kvp = nonFlattenResults.[i] 
            let finalArrayForKey = Array.zeroCreate (kvp.Value |> Seq.sumBy (fun x -> x.Count))
            let mutable finalArrayOffset = 0
            for bucket in kvp.Value do
                bucket.CopyTo(finalArrayForKey,finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + bucket.Count
           
            results.[i] <- (kvp.Key,finalArrayForKey)
        ) |> ignore    
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
//[<ShortRunJob>]//[<DryJob>]  // Uncomment heere for quick local testing
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


    [<Benchmark()>]
    member this.SortThenCreateGroups () = 
        this.ArrayWithItems |> CustomImpl.sortThenCreateGroups ('T.Projection())

    [<Benchmark()>]
    member this.GroupByInPlaceViaSort () = 
        this.ArrayWithItems |> CustomImpl.groupByInPlaceViaSort ('T.Projection())

    [<Benchmark()>]
    member this.GroupByInPlaceViaSortAndParallelSegmentation () = 
        this.ArrayWithItems |> CustomImpl.groupByInPlaceViaSortAndParallelSegmentation ('T.Projection())


    [<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMerge ('T.Projection())








#if Hide_These_Guys



    //[<Benchmark>]
    member this.PlinqSortForReference () = 
        this.ArrayWithItems |> PLINQImplementation.sortBy ('T.Projection())

    //[<Benchmark()>]
    member this.SortJustForReference () = 
        this.ArrayWithItems |> SequentialImplementation.sortBy ('T.Projection())
    (*
    DEAD SECTION
    -------------
    Approaches already being worse in all dimensions by any of the above
    --------------
    *)

    //[<Benchmark()>]
    member this.SortThenCreateGroupsToArray () = 
        this.ArrayWithItems |> CustomImpl.sortThenCreateGroupsToArray ('T.Projection())

    //[<Benchmark>]
    member this.ParallelForImpl () = 
        this.ArrayWithItems |> JunkyardOfBadIdeas.fullyOnParallelFor ('T.Projection())

    //[<Benchmark>] - slow because of list (:: , @) allocations and copies
    member this.EachChunkSeparatelyThenMergeViaList () = 
        this.ArrayWithItems |> JunkyardOfBadIdeas.eachChunkSeparatelyViaList ('T.Projection())

    //[<Benchmark>] - in general slower than EachChunkSeparatelyThenMerge
    member this.CountByThenAssign () = 
        this.ArrayWithItems |> JunkyardOfBadIdeas.countByThenAssign ('T.Projection())

    //[<Benchmark>] - too many allocations, doing work twice
    member this.AtLestProjectionInParallel () = 
        this.ArrayWithItems |> JunkyardOfBadIdeas.atLeastProjectionInParallel ('T.Projection())

    //[<Benchmark>] - lock contentions are too big of an disadvantage
    member this.ConcurrentDictOfBags () = 
        this.ArrayWithItems |> JunkyardOfBadIdeas.concurrentMultiDictionairy ('T.Projection())

#endif

[<EntryPoint>]
let main argv =
    BenchmarkSwitcher.FromTypes([|typedefof<ArrayParallelGroupByBenchMark<ReferenceRecord> >|]).Run(argv) |> ignore   
    0