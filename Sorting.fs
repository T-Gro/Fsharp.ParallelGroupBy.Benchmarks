module Sorting

open System
open System.Threading.Tasks



let private prepareSortedRunsInPlace (array:array<_>) (keysArray:array<_>) projection =
    let partitions = Shared.createPartitions array
    let keyComparer = LanguagePrimitives.FastGenericComparer

    Parallel.For(
        0,
        partitions.Length,
        fun i -> 
            let p = partitions[i]
            let finalIdx = p.Offset+p.Count-1
            for i=p.Offset to finalIdx do
                keysArray[i] <- (projection array[i])

            Array.Sort<_, _>(keysArray, array, partitions[i].Offset, partitions[i].Count, keyComparer)
    )
    |> ignore

    partitions

let inline swap leftIdx rightIdx (array: 'T[]) =
    let temp = array[leftIdx]
    array[leftIdx] <- array[rightIdx]
    array[rightIdx] <- temp

let private mergeTwoSortedConsequtiveSegmentsInPlaceByKeys
    (keysArray: 'TKey[])
    (left: ArraySegment<'T>)
    (right: ArraySegment<'T>)
    =
    let mutable leftIdx = left.Offset
    let leftMax, rightMax = left.Offset + left.Count, right.Offset + right.Count

    while leftIdx < leftMax do
        while (leftIdx < leftMax) && (compare keysArray[leftIdx] keysArray[right.Offset]) <= 0 do
            leftIdx <- leftIdx + 1

        let leftMostUnprocessed = keysArray[leftIdx]
        let mutable writableRightIdx = right.Offset

        while (writableRightIdx < rightMax)
                && (compare leftMostUnprocessed keysArray[writableRightIdx]) > 0 do
            keysArray |> swap leftIdx writableRightIdx
            left.Array |> swap leftIdx writableRightIdx
            writableRightIdx <- writableRightIdx + 1
            leftIdx <- leftIdx + 1

    new ArraySegment<'T>(left.Array, left.Offset, left.Count + right.Count)

let rec mergeRunsInParallel (segmentsInOrder: ArraySegment<'T>[]) pairwiseMerger =
    match segmentsInOrder with
    | [| singleRun |] -> singleRun
    | [| first; second |] -> pairwiseMerger first second
    | [||] -> invalidArg "runs" LanguagePrimitives.ErrorStrings.InputArrayEmptyString
    | threeOrMoreSegments ->
        let mutable left = None
        let mutable right = None
        let midIndex = threeOrMoreSegments.Length / 2

        Parallel.Invoke(
            (fun () -> left <- Some(mergeRunsInParallel threeOrMoreSegments[0 .. midIndex - 1] pairwiseMerger)),
            (fun () -> right <- Some(mergeRunsInParallel threeOrMoreSegments[midIndex..] pairwiseMerger))
        )

        pairwiseMerger left.Value right.Value



let sortInPlaceBy (projection: 'T -> 'U) (array: 'T[]) =

    if array.Length < Shared.sequentialCutoffForGrouping then
        Array.sortInPlaceBy projection array
        Array.map projection array
    else
        let projectedFields = Array.zeroCreate array.Length
        let preSortedPartitions = prepareSortedRunsInPlace array projectedFields projection

        mergeRunsInParallel preSortedPartitions (mergeTwoSortedConsequtiveSegmentsInPlaceByKeys projectedFields)
        |> ignore

        projectedFields