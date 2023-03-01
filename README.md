# Fsharp.ParallelGroupBy.Benchmarks
Benchmarks for implementation of Array.Parallel.GroupBy


There are two main baselines to compare with:
- Array.groupBy ; especially for smaller sizes (<100K) OR very cheap projection. Has the lowest allocation profile
- PLINQ's GroupBy, especially for bigger sizes (>100K) OR expensive projections


The benchmark uses three main profiles which differ in:
- The number of final groups/buckets (controlled by modulo operation)
- Time complexity of the projection. More complex gives advantage to parallel scenarios, since the projection function can be parallelized.
- Especially for the combination of **many final buckets** AND **having a cheap projection function** the inner overhead of .groupBy implementation stands out.

```fsharp
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

```
