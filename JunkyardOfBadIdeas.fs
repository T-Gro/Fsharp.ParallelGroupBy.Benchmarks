module JunkyardOfBadIdeas

open System
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks

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
    let counts = new ConcurrentDictionary<_,_>(concurrencyLevel = Environment.ProcessorCount, capacity = 20*2)
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


let fullyOnParallelFor projection array =
    let resultsBag = new ConcurrentBag<_>()

    Parallel.For(0, Array.length array, 
        localInit = (fun () -> new Dictionary<_,ResizeArray<_>>()), 
        body = (fun idx _ (pLocal:Dictionary<_,ResizeArray<_>>) ->
            let x = array.[idx]
            let key = projection x
            match pLocal.TryGetValue(key) with
            | true, bucket -> bucket.Add(x)
            | false, _ -> 
                let newList = new ResizeArray<_>(1)  
                newList.Add(x)
                pLocal.Add(key,newList)
            pLocal ), 
        localFinally = (fun pLocal -> resultsBag.Add(pLocal))) |> ignore    
         

    let resultsPerChunk = resultsBag.ToArray()

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

let eachChunkSeparatelyViaList projection array =     

    let partitions = Shared.createPartitions array
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
    )  |> ignore   

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
    ) |> ignore   
      
    results
