open System
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Diagnostics.Windows.Configs;


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
        let projectedFields = Sorting.sortInPlaceByAndReturnFields projection array      
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
        let projectedFields = Sorting.sortInPlaceByAndReturnFields projection array      

        let partitions = Shared.createPartitions array
        let possiblyOverlappingSegments = Array.zeroCreate partitions.Length
      

        Parallel.For(0, partitions.Length, fun partitionIdx ->
            let segments = new ResizeArray<_>()
            let partition = partitions[partitionIdx]
            let mutable lastKey = projectedFields.[partition.Offset]
            let mutable lastKeyIndex = partition.Offset
            for elementIdx=(partition.Offset+1) to (partition.Offset + partition.Count - 1) do
                let currentKey = projectedFields.[elementIdx]
                if currentKey <> lastKey then
                    segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, elementIdx - lastKeyIndex))
                    lastKey <- currentKey
                    lastKeyIndex <- elementIdx  

            segments.Add(lastKey,new ArraySegment<'T>(array,lastKeyIndex, (partition.Offset+partition.Count)-lastKeyIndex))
            possiblyOverlappingSegments[partitionIdx] <- segments
        ) |> ignore

        let allSegments = possiblyOverlappingSegments[0]
        allSegments.EnsureCapacity(possiblyOverlappingSegments |> Array.sumBy (fun ra -> ra.Count)) |> ignore
        for i=1 to (possiblyOverlappingSegments.Length-1) do
            let currentSegment = possiblyOverlappingSegments[i]
            let (prevLastKey,prevLastSegment) = allSegments[allSegments.Count-1]
            let (thisFirstKey,thisFirstSegment) = currentSegment[0]

            if prevLastKey <> thisFirstKey then
                allSegments.AddRange(currentSegment)
            else
                allSegments[allSegments.Count-1] <- prevLastKey,new ArraySegment<_>(array,prevLastSegment.Offset, prevLastSegment.Count + thisFirstSegment.Count)
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

    [<Struct>]
    type SmallResizeArray<'T> =
        | Single of onlyOne:'T
        | Double of first:'T * second:'T
        | ThreeOrMore of collection:ResizeArray<'T>

        with 
            member this.Count = 
                match this with
                | Single _ -> 1
                | Double _ -> 2
                | ThreeOrMore col -> col.Count
            member this.CopyTo(target:'T array, idx:int) =
                match this with
                | Single x -> target[idx] <- x
                | Double (f,s) -> 
                    target[idx] <- f
                    target[idx+1] <- s
                | ThreeOrMore col -> col.CopyTo(target,idx)
            member this.Add(item:'T) =
                match this with
                | Single x -> Double(x,item)
                | Double (f,s) -> 
                    let nra = new ResizeArray<_>()
                    nra.Add(f)
                    nra.Add(s)
                    nra.Add(item)
                    ThreeOrMore(nra)
                | ThreeOrMore col -> 
                    col.Add(item)
                    this


    let createPerChunkResults projection array = 
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

        resultsPerChunk

    let inline createPerChunkResultsUsingSmallResizeArray projection array = 
        let partitions = Shared.createPartitions array
        let resultsPerChunk = Array.zeroCreate partitions.Length
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = new Dictionary<_,SmallResizeArray<_>>(partitions.Length)            
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, ThreeOrMore col -> col.Add(x)
                | true, oneOrTwo -> localDict[key] <- oneOrTwo.Add(x)
                | false, _ -> localDict.Add(key,Single(x))  

            resultsPerChunk[i] <- localDict.ToArray() ) |> ignore 

        resultsPerChunk

    let inline combineChunksFromThreads (resultsPerChunk:Dictionary<_,ResizeArray<_>> array) : Dictionary<_,ResizeArray<ResizeArray<_>>> =
        // O ( threads * groups)
        let allResults = new Dictionary<_,ResizeArray<ResizeArray<_>>>()  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, bucket -> bucket.Add(kvp.Value)
                | false, _ -> 
                    let newBuck = new ResizeArray<_>(resultsPerChunk.Length) // MAX. one per partition, in theory less
                    newBuck.Add(kvp.Value)
                    allResults.Add(kvp.Key,newBuck)
        allResults

    let inline combineChunksFromThreadsUsingSmall (resultsPerChunk:array<KeyValuePair<_,SmallResizeArray<_>>> array) : Dictionary<_,SmallResizeArray<SmallResizeArray<_>>> =
        // O ( threads * groups)
        let allResults = new Dictionary<_,SmallResizeArray<SmallResizeArray<_>>>()  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, ThreeOrMore col -> col.Add(kvp.Value)
                | true, oneOrTwo -> allResults[kvp.Key] <- oneOrTwo.Add(kvp.Value)
                | false, _ -> allResults.Add(kvp.Key, Single kvp.Value)  

        allResults

    let inline combineChunksFromThreadsViaSort (resultsPerChunk:Dictionary<'TKey,ResizeArray<'TVal>> array) : (('TKey * 'TVal array)array) =
        // O ( threads * groups)

        let flattened = resultsPerChunk |> Array.collect (fun d -> d.ToArray())
        flattened |> Array.sortInPlaceBy (fun kvp -> kvp.Key)

        let segments = new ResizeArray<_>(1024)

        let addSegment key firstOffset lastOffset = 
            let mutable count = 0
            for i=firstOffset to lastOffset do
                count <- count + flattened[i].Value.Count

            let finalArrayForThisKey = Array.zeroCreate count
            let mutable finalArrayOffset = 0
            for i=firstOffset to lastOffset do
                let currentList = flattened[i].Value
                currentList.CopyTo(finalArrayForThisKey,finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + currentList.Count

            segments.Add(key,finalArrayForThisKey)

        let mutable lastKey = flattened.[0].Key
        let mutable lastKeyIndex = 0
        for i=1 to flattened.Length-1 do
            let currentKey = flattened.[i].Key
            if currentKey <> lastKey then
                addSegment lastKey lastKeyIndex (i-1)         
                lastKey <- currentKey
                lastKeyIndex <- i

        addSegment lastKey lastKeyIndex (flattened.Length-1)

        
        segments.ToArray()

    let inline combineChunksFromThreadsViaSortAndSmallResizeArray (resultsPerChunk:array<KeyValuePair<'TKey,SmallResizeArray<'TVal>>> array) : (('TKey * 'TVal array)array) =
        // O ( threads * groups)

        let flattened = resultsPerChunk |> Array.collect id
        flattened |> Array.sortInPlaceBy (fun kvp -> kvp.Key)

        let segments = new ResizeArray<_>(resultsPerChunk[0].Length)

        let addSegment key firstOffset lastOffset = 
            let mutable count = 0
            for i=firstOffset to lastOffset do
                count <- count + flattened[i].Value.Count

            let finalArrayForThisKey = Array.zeroCreate count
            let mutable finalArrayOffset = 0
            for i=firstOffset to lastOffset do
                let currentList = flattened[i].Value
                currentList.CopyTo(finalArrayForThisKey,finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + currentList.Count

            segments.Add(key,finalArrayForThisKey)

        let mutable lastKey = flattened.[0].Key
        let mutable lastKeyIndex = 0
        for i=1 to flattened.Length-1 do
            let currentKey = flattened.[i].Key
            if currentKey <> lastKey then
                addSegment lastKey lastKeyIndex (i-1)         
                lastKey <- currentKey
                lastKeyIndex <- i

        addSegment lastKey lastKeyIndex (flattened.Length-1)

        
        segments.ToArray()

    let inline createFinalArrays (allResults:Dictionary<_,ResizeArray<ResizeArray<_>>>) = 
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

    let inline createFinalArraysFromSmall (allResults:Dictionary<_,SmallResizeArray<SmallResizeArray<_>>>) = 
        let results = Array.zeroCreate allResults.Count   
        let nonFlattenResults = allResults.ToArray()

        Parallel.For(0,nonFlattenResults.Length, fun i ->            
            let kvp = nonFlattenResults.[i] 
            let finalArrayForKey = Array.zeroCreate (match kvp.Value with | Single x -> x.Count | Double (f,s) -> f.Count + s.Count | ThreeOrMore col -> col |> Seq.sumBy (fun c -> c.Count))
            let mutable finalArrayOffset = 0
            match kvp.Value with
            | Single x ->  
                x.CopyTo(finalArrayForKey, finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + x.Count
            | Double (f,s) -> 
                f.CopyTo(finalArrayForKey, finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + f.Count
                s.CopyTo(finalArrayForKey, finalArrayOffset)
                finalArrayOffset <- finalArrayOffset + s.Count
            | ThreeOrMore col ->
                for colIdx=0 to (col.Count-1) do
                    let bucket = col[colIdx]
                    bucket.CopyTo(finalArrayForKey,finalArrayOffset)
                    finalArrayOffset <- finalArrayOffset + bucket.Count
           
            results.[i] <- (kvp.Key,finalArrayForKey)
        ) |> ignore    
        results


    let inline eachChunkSeparatelyThenMerge projection array =    
        array
        |> createPerChunkResults projection   // O (N / threads)  
        |> combineChunksFromThreads           // O ( threads * groups)
        |> createFinalArrays                  // O(N)

    let inline eachChunkSeparatelyThenMergeUsingSmall projection array =    
        array
        |> createPerChunkResultsUsingSmallResizeArray projection   // O (N / threads)  
        |> combineChunksFromThreadsUsingSmall           // O ( threads * groups)
        |> createFinalArraysFromSmall                // O(N)
      
    let inline eachChunkSeparatelyThenMergeUsingSort projection array =    
        array
        |> createPerChunkResults projection   // O (N / threads)  
        |> combineChunksFromThreadsViaSort           // O ( threads * groups)   
        
    let inline eachChunkSeparatelyThenMergeUsingSortAndSmallArray projection array =    
        array
        |> createPerChunkResultsUsingSmallResizeArray projection   // O (N / threads)  
        |> combineChunksFromThreadsViaSortAndSmallResizeArray           // O ( threads * groups) 

type IBenchMarkElement<'T when 'T :> IBenchMarkElement<'T>> =
    static abstract Create: int * float -> 'T
    static abstract Projection: unit -> ('T -> int)


type ReferenceRecordExpensiveProjection = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecordExpensiveProjection> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Id.ToString().GetHashCode() * (x.Value |> sin |> string |> hash)) % 20

type ReferenceRecordNormal = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecordNormal> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Id  ) % 128

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


type ElementType =
    | StructRecord = 0
    | ReferenceRecord = 1
    | ExpensiveProjection = 2
    | ManyResultingBuckets = 3

[<MemoryDiagnoser>]
//[<EtwProfiler>]
[<ThreadingDiagnoser>]
[<DryJob>]  // Uncomment heere for quick local testing
type ArrayParallelGroupByBenchMarkAllInOne()   = 
    let r = new Random(42)


    [<Params(ElementType.StructRecord,ElementType.ReferenceRecord,ElementType.ExpensiveProjection,ElementType.ManyResultingBuckets)>]   
    member val Type = ElementType.StructRecord with get,set

    [<Params(4_000,100_000,2_500_000)>]      
    member val NumberOfItems = -1 with get,set

    member val ArrayWithItems = Unchecked.defaultof<obj> with get,set

    member this.Create<'T when 'T :> IBenchMarkElement<'T>>() =
        this.ArrayWithItems <- (Array.init this.NumberOfItems (fun idx -> 'T.Create(idx,r.NextDouble()))) :> obj

    member this.Process<'T when 'T :> IBenchMarkElement<'T>>(func:('T->'a) -> array<'T> -> array<'a * array<'T>>) = 
        (this.ArrayWithItems :?> array<'T>) |> func ('T.Projection()) |> Array.length

    [<GlobalSetup>]
    member this.GlobalSetup () = 
        match this.Type with
        | ElementType.StructRecord -> this.Create<StructRecord>()
        | ElementType.ReferenceRecord -> this.Create<ReferenceRecordNormal>()
        | ElementType.ExpensiveProjection -> this.Create<ReferenceRecordExpensiveProjection>()
        | ElementType.ManyResultingBuckets -> this.Create<ReferenceRecordManyBuckets>()

    [<Benchmark(Baseline=true)>]
    member this.ArrayGroupBy () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(Array.groupBy)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(Array.groupBy)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(Array.groupBy)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(Array.groupBy)

    [<Benchmark(Baseline=true)>]
    member this.PLINQDefault () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(PLINQImplementation.groupBy)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(PLINQImplementation.groupBy)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(PLINQImplementation.groupBy)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(PLINQImplementation.groupBy)

    [<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(CustomImpl.eachChunkSeparatelyThenMerge)
             

[<MemoryDiagnoser>]
//[<EtwProfiler>]
[<ThreadingDiagnoser>]
[<GenericTypeArguments(typeof<ReferenceRecordExpensiveProjection>)>]
[<GenericTypeArguments(typeof<ReferenceRecordNormal>)>]
[<GenericTypeArguments(typeof<ReferenceRecordManyBuckets>)>]
[<GenericTypeArguments(typeof<StructRecord>)>]
//[<DryJob>]  // Uncomment heere for quick local testing
type ArrayParallelGroupByBenchMark<'T when 'T :> IBenchMarkElement<'T>>() = 

    let r = new Random(42)

    [<Params(4_000,100_000,2_500_000)>]      
    member val NumberOfItems = -1 with get,set

    member val ArrayWithItems = Unchecked.defaultof<'T[]> with get,set

    [<GlobalSetup>]
    member this.GlobalSetup () = 
        this.ArrayWithItems <- Array.init this.NumberOfItems (fun idx -> 'T.Create(idx,r.NextDouble()))        

    //[<Benchmark(Baseline = true)>]
    member this.Sequential () = 
        this.ArrayWithItems |> SequentialImplementation.groupBy ('T.Projection())

    [<Benchmark>]
    member this.PLINQDefault () = 
        this.ArrayWithItems |> PLINQImplementation.groupBy ('T.Projection())


    //[<Benchmark()>]
    member this.SortThenCreateGroups () = 
        this.ArrayWithItems |> CustomImpl.sortThenCreateGroups ('T.Projection())

    //[<Benchmark()>]
    member this.GroupByInPlaceViaSort () = 
        this.ArrayWithItems |> CustomImpl.groupByInPlaceViaSort ('T.Projection())

    [<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMerge ('T.Projection())

    //[<Benchmark>]
    member this.DivideSmallConquer () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMergeUsingSmall ('T.Projection())

    //[<Benchmark>]
    member this.EachChunkSeparatelyThenMergeUsingSort () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMergeUsingSort ('T.Projection())

    //[<Benchmark>]
    member this.DivideViaSmallRAThenSort () = 
        this.ArrayWithItems |> CustomImpl.eachChunkSeparatelyThenMergeUsingSortAndSmallArray ('T.Projection())

    //[<Benchmark()>]
    member this.GroupByInPlaceViaSortAndParallelSegmentation () = 
        this.ArrayWithItems |> CustomImpl.groupByInPlaceViaSortAndParallelSegmentation ('T.Projection())








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
    BenchmarkRunner.Run<ArrayParallelGroupByBenchMarkAllInOne>() |> ignore    
    //BenchmarkSwitcher.FromTypes([|typedefof<ArrayParallelGroupByBenchMark<ReferenceRecordNormal> >|]).Run(argv) |> ignore   
    0