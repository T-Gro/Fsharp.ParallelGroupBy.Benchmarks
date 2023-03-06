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

    type ArrayBuilder<'T when 'T:equality>(singleElemArray) = 
        let mutable _linkedList : list<'T array> = []
        let mutable _tail : 'T array = singleElemArray
        let mutable _tailIdx = 1

        member _.Add(item:'T) = 
            if _tailIdx = _tail.Length then
                _linkedList <- _tail :: _linkedList
                _tail <- Array.zeroCreate (_tail.Length * 4)
                _tail[0] <- item
                _tailIdx <- 1
            else
                _tail[_tailIdx] <- item
                _tailIdx <- _tailIdx + 1

        member _.GetAsList() = 
            Array.Resize(&_tail,_tailIdx)
            (_tail :: _linkedList)

        member _.CombineWith(other:ArrayBuilder<'T>) =
            _linkedList <- other.GetAsList() @ _linkedList

        member _.Count() = 
            _tailIdx + (_linkedList |> List.sumBy (fun x -> x.Length))

        member _.SumBy(func) = 
            let mutable fromTail = 0
            for i=0 to (_tailIdx-1) do
                fromTail <- fromTail + (func _tail[i])          
            let fromLinkedList = _linkedList |> List.sumBy (fun li -> li |> Array.sumBy func)
            fromLinkedList + fromTail

        member _.CopyTo(destination:'T [],offset : int) =
            Array.Copy(_tail, 0, destination, offset, _tailIdx)

            let mutable destinationOffset = offset + _tailIdx
            _linkedList |> List.iter (fun arr ->
                Array.Copy(arr,0, destination, destinationOffset, arr.Length)           
                destinationOffset <- destinationOffset + arr.Length)

            destinationOffset

        member this.ToArray() =
            let arr = Array.zeroCreate (this.Count())
            this.CopyTo(arr,0) |> ignore
            arr

        member _.Iter(action) = 
            for i=0 to (_tailIdx-1) do
                action _tail[i]
            _linkedList |> List.iter (fun li -> li |> Array.iter action)
            

    [<Struct>]
    type SmallResizeArray<'T when 'T:equality> =
        | Single of onlyOne:'T    
        | TwoOrMore of collection:ArrayBuilder<'T>

        with 
            member this.Count = 
                match this with
                | Single _ -> 1            
                | TwoOrMore col -> col.Count()
            member this.SumBy(func) = 
                match this with
                | Single x -> func x           
                | TwoOrMore col -> col.SumBy(func)
            member this.CopyTo(target:'T array, idx:int) =
                match this with
                | Single x -> 
                    target[idx] <- x             
                    idx + 1
                | TwoOrMore col -> col.CopyTo(target,idx)
            member this.Add(item:'T) =
                match this with
                | Single x -> TwoOrMore(new ArrayBuilder<_>([|x;item|]))               
                | TwoOrMore col -> 
                    col.Add(item)
                    this


    let inline createPerChunkResults shouldSort ([<InlineIfLambda>]projection) array = 
        let partitions = Shared.createPartitions array
        let resultsPerChunk = Array.zeroCreate partitions.Length
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = new Dictionary<_,ArrayBuilder<_>>(chunk.Count)
            
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, bucket -> bucket.Add(x)
                | false, _ -> localDict.Add(key,new ArrayBuilder<_>(Array.singleton x))
            resultsPerChunk[i] <- localDict.ToArray()
            if shouldSort then
                resultsPerChunk[i] |> Array.sortInPlaceBy (fun kvp -> kvp.Key)) |> ignore   

        resultsPerChunk

    let inline createPerChunkResultsUsingSmallResizeArray ([<InlineIfLambda>]projection) array = 
        let partitions = Shared.createPartitions array
        let resultsPerChunk = Array.zeroCreate partitions.Length
        // O (N / threads)      
        Parallel.For(0,partitions.Length, fun i ->            
            let chunk = partitions[i]
            let localDict = new Dictionary<_,SmallResizeArray<_>>(chunk.Count)            
            let lastOffset = chunk.Offset + chunk.Count - 1 
            for i=chunk.Offset to lastOffset do
                let x = array.[i]
                let key = projection x
                match localDict.TryGetValue(key) with
                | true, TwoOrMore col -> col.Add(x)
                | true, oneOrTwo -> localDict[key] <- oneOrTwo.Add(x)
                | false, _ -> localDict.Add(key,Single(x))  

            resultsPerChunk[i] <- localDict.ToArray() ) |> ignore 

        resultsPerChunk

    let combineFromThreadsViaSort  (resultsPerChunk:array<KeyValuePair<_,ArrayBuilder<_>>> array) =

        let mergeTogether          
            (left: array<KeyValuePair<_,ArrayBuilder<_>>>)
            (right: array<KeyValuePair<_,ArrayBuilder<_>>>)
            =

            let mutable resultArray = Array.zeroCreate (left.Length + right.Length)
            let mutable resultPointer = 0
            let mutable leftPointer = 0
            let mutable rightPointer = 0

            while (leftPointer < left.Length && rightPointer < right.Length) do
                match compare (left[leftPointer].Key) (right[rightPointer].Key) with
                | 0 -> 
                    let kvp = left[leftPointer]
                    kvp.Value.CombineWith(right[rightPointer].Value)
                    resultArray[resultPointer] <- kvp
                    leftPointer <- leftPointer + 1
                    rightPointer <- rightPointer + 1
                | neg when neg < 0 ->
                    resultArray[resultPointer] <- left[leftPointer]
                    leftPointer <- leftPointer + 1
                | positive -> 
                    resultArray[resultPointer] <- right[rightPointer]
                    rightPointer <- rightPointer + 1

                resultPointer <- resultPointer + 1

            while(leftPointer < left.Length) do
                resultArray[resultPointer] <- left[leftPointer]
                resultPointer <- resultPointer + 1
                leftPointer <- leftPointer + 1

            while(rightPointer < right.Length) do
                resultArray[resultPointer] <- right[rightPointer]
                resultPointer <- resultPointer + 1
                rightPointer <- rightPointer + 1


            Array.Resize(&resultArray, resultPointer)
            resultArray

        let rec mergeRunsInParallel (resultsPerChunk:array<KeyValuePair<_,ArrayBuilder<_>>> array) =
            match resultsPerChunk with
            | [| singleRun |] -> singleRun
            | [| first; second |] -> mergeTogether first second
            | [||] -> invalidArg "runs" LanguagePrimitives.ErrorStrings.InputArrayEmptyString
            | threeOrMoreSegments ->
                let mutable left = None
                let mutable right = None
                let midIndex = threeOrMoreSegments.Length / 2

                Parallel.Invoke(
                    (fun () -> left <- Some(mergeRunsInParallel threeOrMoreSegments[0 .. midIndex - 1] )),
                    (fun () -> right <- Some(mergeRunsInParallel threeOrMoreSegments[midIndex..] ))
                )

                mergeTogether left.Value right.Value

        resultsPerChunk
        |> mergeRunsInParallel
        |> Array.Parallel.map (fun x -> (x.Key, x.Value.ToArray()))


    let combineChunksFromThreads (resultsPerChunk:array<KeyValuePair<_,ArrayBuilder<_>>> array) : Dictionary<_,ResizeArray<ArrayBuilder<_>>> =
        // O ( threads * groups)
        let allResults = new Dictionary<_,ResizeArray<ArrayBuilder<_>>>(resultsPerChunk[0].Length)  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, bucket -> bucket.Add(kvp.Value)
                | false, _ -> 
                    let newBuck = new ResizeArray<_>(resultsPerChunk.Length-i) // MAX. one per partition, in theory less
                    newBuck.Add(kvp.Value)
                    allResults.Add(kvp.Key,newBuck)
        allResults

    let combineArrayBuildersFromThreads (resultsPerChunk:array<KeyValuePair<_,ArrayBuilder<_>>> array) : Dictionary<_,ArrayBuilder<_>> =
        // O ( threads * groups)
        let allResults = new Dictionary<_,ArrayBuilder<_>>(resultsPerChunk[0].Length)  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, bucket -> bucket.CombineWith(kvp.Value)
                | false, _ -> allResults.Add(kvp.Key,kvp.Value)
        allResults
    
    let buildersToArray (d:Dictionary<_,ArrayBuilder<_>>) = 
        let arr = Array.zeroCreate d.Count
        let mutable idx = 0
        for kvp in d do
            arr[idx] <- (kvp.Key, kvp.Value.ToArray())
            idx <- idx + 1
        arr    

    let combineChunksFromThreadsUsingSmall (resultsPerChunk:array<KeyValuePair<_,SmallResizeArray<_>>> array) : Dictionary<_,SmallResizeArray<SmallResizeArray<_>>> =
        // O ( threads * groups)
        let allResults = new Dictionary<_,SmallResizeArray<SmallResizeArray<_>>>(resultsPerChunk[0].Length)  // one entry per final key
        for i=0 to resultsPerChunk.Length-1 do
            let result = resultsPerChunk.[i]
            for kvp in result do
                match allResults.TryGetValue(kvp.Key) with
                | true, TwoOrMore col -> col.Add(kvp.Value)
                | true, one -> allResults[kvp.Key] <- one.Add(kvp.Value)
                | false, _ -> allResults.Add(kvp.Key, Single kvp.Value)  

        allResults

    let combineChunksFromThreadsViaSort (resultsPerChunk:Dictionary<'TKey,ArrayBuilder<'TVal>> array) : (('TKey * 'TVal array)array) =
        // O ( threads * groups)

        let flattened = resultsPerChunk |> Array.collect (fun d -> d.ToArray())
        flattened |> Array.sortInPlaceBy (fun kvp -> kvp.Key)

        let segments = new ResizeArray<_>(1024)

        let addSegment key firstOffset lastOffset = 
            let mutable count = 0
            for i=firstOffset to lastOffset do
                count <- count + flattened[i].Value.Count()

            let finalArrayForThisKey = Array.zeroCreate count
            let mutable finalArrayOffset = 0
            for i=firstOffset to lastOffset do
                let currentList = flattened[i].Value
                finalArrayOffset <- currentList.CopyTo(finalArrayForThisKey,finalArrayOffset)

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

    let combineChunksFromThreadsViaSortAndSmallResizeArray (resultsPerChunk:array<KeyValuePair<'TKey,SmallResizeArray<'TVal>>> array) : (('TKey * 'TVal array)array) =
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
                finalArrayOffset <- currentList.CopyTo(finalArrayForThisKey,finalArrayOffset)

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

    let createFinalArrays (allResults:Dictionary<_,ResizeArray<ArrayBuilder<_>>>) = 
        let results = Array.zeroCreate allResults.Count   
        let nonFlattenResults = allResults.ToArray()

        Parallel.For(0,nonFlattenResults.Length, fun i ->            
            let kvp = nonFlattenResults.[i] 
            let finalArrayForKey = Array.zeroCreate (kvp.Value |> Seq.sumBy (fun x -> x.Count()))
            let mutable finalArrayOffset = 0
            for bucket in kvp.Value do
                finalArrayOffset <- bucket.CopyTo(finalArrayForKey,finalArrayOffset)   

            results.[i] <- (kvp.Key,finalArrayForKey)
        ) |> ignore    
        results

    let createFinalArraysFromSmall (allResults:Dictionary<_,SmallResizeArray<SmallResizeArray<_>>>) = 
        let results = Array.zeroCreate allResults.Count   
        let nonFlattenResults = allResults.ToArray()

        Parallel.For(0,nonFlattenResults.Length, fun i ->            
            let kvp = nonFlattenResults.[i] 
            let finalArrayForKey = Array.zeroCreate (kvp.Value.SumBy (fun x -> x.Count))
            let mutable finalArrayOffset = 0
            match kvp.Value with
            | Single x ->  
                finalArrayOffset <- x.CopyTo(finalArrayForKey, finalArrayOffset)        
            | TwoOrMore col ->
                col.Iter( fun bucket -> finalArrayOffset <- bucket.CopyTo(finalArrayForKey,finalArrayOffset))
           
            results.[i] <- (kvp.Key,finalArrayForKey)
        ) |> ignore    
        results


    let inline eachChunkSeparatelyThenMerge projection array =    
        array
        |> createPerChunkResults false projection   // O (N / threads)  
        |> combineChunksFromThreads           // O ( threads * groups)
        |> createFinalArrays                  // O(N)

    let inline eachChunkSeparatelyThenCombineArrayBuilders  projection array =
        array
        |> createPerChunkResults false projection
        |> combineArrayBuildersFromThreads
        |> buildersToArray

    let inline eachChunkSeparatelyThenMergeUsingSmall projection array =    
        array
        |> createPerChunkResultsUsingSmallResizeArray projection   // O (N / threads)  
        |> combineChunksFromThreadsUsingSmall           // O ( threads * groups)
        |> createFinalArraysFromSmall                // O(N)
      
    let inline eachChunkSeparatelyThenMergeUsingSort projection array =    
        array
        |> createPerChunkResults true projection   // O (N / threads)  
        |> combineFromThreadsViaSort           // O ( threads * groups)   
        
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

[<Struct>]
type StructRecordManyBuckets = {Id : int; Value : float}
    with interface IBenchMarkElement<StructRecordManyBuckets> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> x.Id % 10_000


type ElementType =
    | StructRecord = 0
    | ReferenceRecord = 1
    | ExpensiveProjection = 2
    | ManyResultingBuckets = 3
    | StructRecordManyBuckets = 4

[<MemoryDiagnoser>]
//[<EtwProfiler>]
[<ThreadingDiagnoser>]
//[<ShortRunJob>]
//[<DryJob>]  // Uncomment heere for quick local testing
type ArrayParallelGroupByBenchMarkAllInOne()   = 
    let r = new Random(42)


    [<Params(ElementType.StructRecord,ElementType.ReferenceRecord,ElementType.ExpensiveProjection,ElementType.ManyResultingBuckets, ElementType.StructRecordManyBuckets)>]   
    member val Type = ElementType.StructRecord with get,set

    //[<Params(4_000,50_000,100_000,500_000,2_500_000, 25_000_000)>]     
    [<Params(4_000,50_000,100_000,250_000,500_000)>]  
    member val NumberOfItems = -1 with get,set

    member val ArrayWithItems = Unchecked.defaultof<obj> with get,set

    member this.Create<'T when 'T :> IBenchMarkElement<'T>>() =
        this.ArrayWithItems <- (Array.init this.NumberOfItems (fun idx -> 'T.Create(idx,r.NextDouble()))) :> obj

    member this.Process<'T when 'T :> IBenchMarkElement<'T>>(func:('T->'a) -> array<'T> -> array<'a * array<'T>>) = 
        (this.ArrayWithItems :?> array<'T>) |> func ('T.Projection()) |> Array.length

    member this.ProcessWithSegments<'T when 'T :> IBenchMarkElement<'T>>(func:('T->'a) -> array<'T> -> array<'a * ArraySegment<'T>>) = 
        (this.ArrayWithItems :?> array<'T>) |> func ('T.Projection()) |> Array.length

    [<GlobalSetup>]
    member this.GlobalSetup () = 
        match this.Type with
        | ElementType.StructRecord -> this.Create<StructRecord>()
        | ElementType.ReferenceRecord -> this.Create<ReferenceRecordNormal>()
        | ElementType.ExpensiveProjection -> this.Create<ReferenceRecordExpensiveProjection>()
        | ElementType.ManyResultingBuckets -> this.Create<ReferenceRecordManyBuckets>()
        | ElementType.StructRecordManyBuckets -> this.Create<StructRecordManyBuckets>()

    //[<Benchmark()>]
    member this.SortByForReference () = 
        match this.Type with
        | ElementType.StructRecord -> (this.ArrayWithItems :?> StructRecord[]) |> Array.sortBy (fun x -> x.Value) |> Array.length
        | ElementType.ReferenceRecord -> (this.ArrayWithItems :?> ReferenceRecordNormal[]) |> Array.sortBy (fun x -> x.Value) |> Array.length
        | ElementType.ExpensiveProjection -> (this.ArrayWithItems :?> ReferenceRecordExpensiveProjection[]) |> Array.sortBy (fun x -> x.Value) |> Array.length
        | ElementType.ManyResultingBuckets -> (this.ArrayWithItems :?> ReferenceRecordManyBuckets[]) |> Array.sortBy (fun x -> x.Value) |> Array.length
        | ElementType.StructRecordManyBuckets -> (this.ArrayWithItems :?> StructRecordManyBuckets[]) |> Array.sortBy (fun x -> x.Value) |> Array.length

    [<Benchmark()>]
    member this.ArrayGroupBy () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(Array.groupBy)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(Array.groupBy)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(Array.groupBy)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(Array.groupBy)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(Array.groupBy)

    //[<Benchmark(Baseline=true)>]
    member this.PLINQDefault () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(PLINQImplementation.groupBy)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(PLINQImplementation.groupBy)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(PLINQImplementation.groupBy)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(PLINQImplementation.groupBy)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(PLINQImplementation.groupBy)

    //[<Benchmark>]
    member this.EachChunkSeparatelyThenMerge () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(CustomImpl.eachChunkSeparatelyThenMerge)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(CustomImpl.eachChunkSeparatelyThenMerge)

    //[<Benchmark>]
    member this.EachChunkSeparatelyThenCombineArrayBuilders () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(CustomImpl.eachChunkSeparatelyThenCombineArrayBuilders)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(CustomImpl.eachChunkSeparatelyThenCombineArrayBuilders)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(CustomImpl.eachChunkSeparatelyThenCombineArrayBuilders)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(CustomImpl.eachChunkSeparatelyThenCombineArrayBuilders)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(CustomImpl.eachChunkSeparatelyThenCombineArrayBuilders)

    [<Benchmark>]
    member this.GroupByInPlaceViaSortAndParallelSegmentation () = 
        match this.Type with
        | ElementType.StructRecord -> this.ProcessWithSegments<StructRecord>(CustomImpl.groupByInPlaceViaSortAndParallelSegmentation)
        | ElementType.ReferenceRecord -> this.ProcessWithSegments<ReferenceRecordNormal>(CustomImpl.groupByInPlaceViaSortAndParallelSegmentation)
        | ElementType.ExpensiveProjection -> this.ProcessWithSegments<ReferenceRecordExpensiveProjection>(CustomImpl.groupByInPlaceViaSortAndParallelSegmentation)
        | ElementType.ManyResultingBuckets -> this.ProcessWithSegments<ReferenceRecordManyBuckets>(CustomImpl.groupByInPlaceViaSortAndParallelSegmentation)
        | ElementType.StructRecordManyBuckets -> this.ProcessWithSegments<StructRecordManyBuckets>(CustomImpl.groupByInPlaceViaSortAndParallelSegmentation)
             
    [<Benchmark>]
    member this.SortThenCreateGroupsToArray () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(CustomImpl.sortThenCreateGroupsToArray)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(CustomImpl.sortThenCreateGroupsToArray)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(CustomImpl.sortThenCreateGroupsToArray)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(CustomImpl.sortThenCreateGroupsToArray)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(CustomImpl.sortThenCreateGroupsToArray)

    //[<Benchmark>]
    member this.GroupByInPlaceViaSort () = 
        match this.Type with
        | ElementType.StructRecord -> this.ProcessWithSegments<StructRecord>(CustomImpl.groupByInPlaceViaSort)
        | ElementType.ReferenceRecord -> this.ProcessWithSegments<ReferenceRecordNormal>(CustomImpl.groupByInPlaceViaSort)
        | ElementType.ExpensiveProjection -> this.ProcessWithSegments<ReferenceRecordExpensiveProjection>(CustomImpl.groupByInPlaceViaSort)
        | ElementType.ManyResultingBuckets -> this.ProcessWithSegments<ReferenceRecordManyBuckets>(CustomImpl.groupByInPlaceViaSort)
        | ElementType.StructRecordManyBuckets -> this.ProcessWithSegments<StructRecordManyBuckets>(CustomImpl.groupByInPlaceViaSort)

    //[<Benchmark>]
    member this.FullyOnParallelFor () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(JunkyardOfBadIdeas.fullyOnParallelFor)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(JunkyardOfBadIdeas.fullyOnParallelFor)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(JunkyardOfBadIdeas.fullyOnParallelFor)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(JunkyardOfBadIdeas.fullyOnParallelFor)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(JunkyardOfBadIdeas.fullyOnParallelFor)

    //[<Benchmark>]
    member this.CountByThenAssign () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(JunkyardOfBadIdeas.countByThenAssign)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(JunkyardOfBadIdeas.countByThenAssign)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(JunkyardOfBadIdeas.countByThenAssign)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(JunkyardOfBadIdeas.countByThenAssign)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(JunkyardOfBadIdeas.countByThenAssign)

    [<Benchmark>]
    member this.CountByThenAssignHandRolled () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(JunkyardOfBadIdeas.countByThenAssignHandRolled)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(JunkyardOfBadIdeas.countByThenAssignHandRolled)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(JunkyardOfBadIdeas.countByThenAssignHandRolled)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(JunkyardOfBadIdeas.countByThenAssignHandRolled)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(JunkyardOfBadIdeas.countByThenAssignHandRolled)

    //[<Benchmark>]
    member this.ConcurrentMultiDictionairy () = 
        match this.Type with
        | ElementType.StructRecord -> this.Process<StructRecord>(JunkyardOfBadIdeas.concurrentMultiDictionairy)
        | ElementType.ReferenceRecord -> this.Process<ReferenceRecordNormal>(JunkyardOfBadIdeas.concurrentMultiDictionairy)
        | ElementType.ExpensiveProjection -> this.Process<ReferenceRecordExpensiveProjection>(JunkyardOfBadIdeas.concurrentMultiDictionairy)
        | ElementType.ManyResultingBuckets -> this.Process<ReferenceRecordManyBuckets>(JunkyardOfBadIdeas.concurrentMultiDictionairy)
        | ElementType.StructRecordManyBuckets -> this.Process<StructRecordManyBuckets>(JunkyardOfBadIdeas.concurrentMultiDictionairy)

#if Hide_These_Guys
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

let debugIssues() = 
    let bm = new ArrayParallelGroupByBenchMarkAllInOne()
    bm.NumberOfItems <- 4000
    bm.Type <- ElementType.StructRecord
    bm.GlobalSetup()
    bm.EachChunkSeparatelyThenCombineArrayBuilders()

[<EntryPoint>]
let main argv = 

    BenchmarkRunner.Run<ArrayParallelGroupByBenchMarkAllInOne>() |> ignore    
    //BenchmarkSwitcher.FromTypes([|typedefof<ArrayParallelGroupByBenchMark<ReferenceRecordNormal> >|]).Run(argv) |> ignore   
    0