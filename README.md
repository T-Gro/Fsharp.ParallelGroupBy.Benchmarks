# Fsharp.ParallelGroupBy.Benchmarks
Benchmarks for implementation of Array.Parallel.GroupBy


There are two main baselines to compare with:
- Array.groupBy ; especially for smaller sizes (<100K) OR very cheap projection. Has the lowest allocation profile
- PLINQ's GroupBy, especially for bigger sizes (>100K) OR expensive projections


The benchmark uses three main profiles which differ in:
- The number of final groups/buckets (controlled by modulo operation)
- Time complexity of the projection. More complex gives advantage to parallel scenarios, since the projection function can be parallelized.
- Especially for the combination of **many final buckets** AND **having a cheap projection function** the inner overhead of .groupBy implementation stands out.

## Expensive projection, low number of buckets
```fsharp
type ReferenceRecord = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecord> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Id.ToString().GetHashCode() * (x.Value |> sin |> string |> hash)) % 20
                
```
BenchmarkDotNet=v0.13.4, OS=Windows 11 (10.0.22621.1265)
11th Gen Intel Core i9-11950H 2.60GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK=7.0.200
  [Host]     : .NET 7.0.3 (7.0.323.6910), X64 RyuJIT AVX2 DEBUG
  DefaultJob : .NET 7.0.3 (7.0.323.6910), X64 RyuJIT AVX2


|                       Method | NumberOfItems |           Mean |        Error |       StdDev | Ratio | RatioSD | Completed Work Items | Lock Contentions |        Gen0 |      Gen1 |     Gen2 |     Allocated | Alloc Ratio |
|----------------------------- |-------------- |---------------:|-------------:|-------------:|------:|--------:|---------------------:|-----------------:|------------:|----------:|---------:|--------------:|------------:|
|                   Sequential |          4000 |       728.5 us |     14.47 us |     29.89 us |  1.00 |    0.00 |                    - |                - |     40.0391 |   10.7422 |        - |     495.83 KB |        1.00 |
|                 PLINQDefault |          4000 |       365.4 us |      2.25 us |      1.88 us |  0.49 |    0.02 |              15.0000 |          11.7705 |     92.2852 |   74.2188 |        - |    1066.61 KB |        2.15 |
| EachChunkSeparatelyThenMerge |          4000 |       207.0 us |      2.96 us |      2.63 us |  0.28 |    0.01 |               8.9529 |           0.0015 |     44.4336 |   18.3105 |        - |     542.93 KB |        1.09 |
|            CountByThenAssign |          4000 |       352.6 us |      3.14 us |      2.78 us |  0.47 |    0.02 |              39.3853 |           0.0166 |     85.9375 |   17.0898 |        - |    1049.86 KB |        2.12 |
|                              |               |                |              |              |       |         |                      |                  |             |           |          |               |             |
|                   Sequential |         20000 |     3,737.1 us |     73.26 us |    151.29 us |  1.00 |    0.00 |                    - |                - |    195.3125 |   78.1250 |        - |    2413.88 KB |        1.00 |
|                 PLINQDefault |         20000 |     1,032.2 us |      8.84 us |      7.39 us |  0.28 |    0.02 |              15.0000 |          11.7500 |    265.6250 |  148.4375 |        - |    3082.15 KB |        1.28 |
| EachChunkSeparatelyThenMerge |         20000 |       827.5 us |      5.99 us |      5.00 us |  0.22 |    0.01 |               8.9941 |           0.0039 |    212.8906 |  105.4688 |        - |    2553.95 KB |        1.06 |
|            CountByThenAssign |         20000 |     1,592.8 us |     10.08 us |      8.42 us |  0.43 |    0.03 |              41.1270 |           0.0391 |    423.8281 |  148.4375 |        - |    5157.01 KB |        2.14 |
|                              |               |                |              |              |       |         |                      |                  |             |           |          |               |             |
|                   Sequential |        100000 |    19,951.3 us |    390.98 us |    684.78 us |  1.00 |    0.00 |                    - |                - |   1031.2500 |  906.2500 |  31.2500 |   12676.34 KB |        1.00 |
|                 PLINQDefault |        100000 |    10,149.3 us |    212.78 us |    600.14 us |  0.50 |    0.03 |              15.0000 |           9.6875 |   1609.3750 | 1000.0000 | 234.3750 |   15463.38 KB |        1.22 |
| EachChunkSeparatelyThenMerge |        100000 |     5,670.2 us |    111.56 us |    124.00 us |  0.29 |    0.01 |               9.0000 |                - |   1078.1250 |  960.9375 |  23.4375 |   12604.91 KB |        0.99 |
|            CountByThenAssign |        100000 |     8,304.8 us |    116.15 us |    108.65 us |  0.42 |    0.02 |              49.1250 |                - |   2171.8750 |  312.5000 |  46.8750 |   25694.42 KB |        2.03 |
|                              |               |                |              |              |       |         |                      |                  |             |           |          |               |             |
|                   Sequential |        500000 |   100,106.9 us |  1,988.80 us |  3,037.11 us |  1.00 |    0.00 |                    - |                - |   4833.3333 | 1500.0000 | 333.3333 |   63881.33 KB |        1.00 |
|                 PLINQDefault |        500000 |    47,543.2 us |    938.26 us |    877.65 us |  0.47 |    0.02 |              15.0000 |           9.5455 |   6818.1818 | 2909.0909 | 727.2727 |   76838.74 KB |        1.20 |
| EachChunkSeparatelyThenMerge |        500000 |    33,370.4 us |    653.23 us |    997.56 us |  0.33 |    0.01 |               9.0000 |                - |   5687.5000 | 1687.5000 | 437.5000 |    65982.7 KB |        1.03 |
|            CountByThenAssign |        500000 |    39,205.0 us |    365.45 us |    341.85 us |  0.39 |    0.01 |              61.4615 |                - |  10846.1538 |  230.7692 |  76.9231 |  134614.89 KB |        2.11 |
|                              |               |                |              |              |       |         |                      |                  |             |           |          |               |             |
|                   Sequential |       2500000 |   487,646.0 us |  9,539.67 us | 17,682.39 us |  1.00 |    0.00 |                    - |                - |  20000.0000 | 2000.0000 |        - |  312221.98 KB |        1.00 |
|                 PLINQDefault |       2500000 |   181,093.7 us |  3,602.78 us |  5,392.47 us |  0.37 |    0.02 |              15.0000 |           9.0000 |  24666.6667 | 5333.3333 | 333.3333 |  369378.12 KB |        1.18 |
| EachChunkSeparatelyThenMerge |       2500000 |   127,302.4 us |  2,099.55 us |  1,963.92 us |  0.26 |    0.01 |               9.0000 |                - |  21400.0000 |  800.0000 | 800.0000 |  332882.07 KB |        1.07 |
|            CountByThenAssign |       2500000 |   185,886.3 us |  2,450.93 us |  2,292.60 us |  0.38 |    0.02 |              69.6667 |           0.3333 |  54000.0000 |         - |        - |  679229.22 KB |        2.18 |
|                              |               |                |              |              |       |         |                      |                  |             |           |          |               |             |
|                   Sequential |      12500000 | 2,300,375.2 us | 29,256.61 us | 27,366.65 us |  1.00 |    0.00 |                    - |                - | 103000.0000 | 2000.0000 |        - | 1687044.56 KB |        1.00 |
|                 PLINQDefault |      12500000 |   757,864.7 us | 14,892.11 us | 27,231.07 us |  0.33 |    0.01 |              15.0000 |          10.0000 | 107000.0000 | 9000.0000 |        - | 1932534.56 KB |        1.15 |
| EachChunkSeparatelyThenMerge |      12500000 |   444,536.3 us |  7,042.56 us |  7,232.19 us |  0.19 |    0.00 |              16.0000 |                - | 103000.0000 |         - |        - | 1667376.37 KB |        0.99 |
|            CountByThenAssign |      12500000 |   944,158.6 us |  9,221.93 us |  8,626.19 us |  0.41 |    0.01 |              99.0000 |                - | 270000.0000 |         - |        - | 3402304.71 KB |        2.02 |

```fsharp
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
