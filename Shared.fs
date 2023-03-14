module Shared

open System

// The following two parameters were benchmarked and found to be optimal.
// Benchmark was run using: 11th Gen Intel Core i9-11950H 2.60GHz, 1 CPU, 16 logical and 8 physical cores
let private maxPartitions = Environment.ProcessorCount  // The maximum number of partitions to use
let sequentialCutoffForGrouping = 2_500 // Arrays smaller then this will be sorted sequentially
let private minChunkSize = 8 // The minimum size of a chunk to be sorted in parallel

let createPartitions (array: 'T[]) =
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
