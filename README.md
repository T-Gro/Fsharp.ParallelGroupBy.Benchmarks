# Fsharp.ParallelGroupBy.Benchmarks
Benchmarks for implementation of Array.Parallel.GroupBy


There are two main baselines to compare with:
- Array.groupBy ; especially for smaller sizes (<100K) OR very cheap projection. Has the lowest allocation profile
- PLINQ's GroupBy, especially for bigger sizes (>100K) OR expensive projections


The benchmark uses three main profiles which differ in:
- The number of final groups/buckets (controlled by modulo operation)
- Time complexity of the projection. More complex gives advantage to parallel scenarios, since the projection function can be parallelized.
- Especially for the combination of **many final buckets** AND **having a cheap projection function** the inner overhead of .groupBy implementation stands out.


## Expensive projection, low number of buckets -> good for parallelism
```fsharp
type ReferenceRecord = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecord> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Id.ToString().GetHashCode() * (x.Value |> sin |> string |> hash)) % 20
                
```


|                       Method | NumberOfItems |         Mean |       Error |       StdDev |       Median | Ratio | RatioSD | Completed Work Items | Lock Contentions |       Gen0 |       Gen1 |     Gen2 |    Allocated | Alloc Ratio |
|----------------------------- |-------------- |-------------:|------------:|-------------:|-------------:|------:|--------:|---------------------:|-----------------:|-----------:|-----------:|---------:|-------------:|------------:|
|                   Sequential |          4000 |     755.7 us |    14.83 us |     31.28 us |     761.1 us |  1.00 |    0.00 |                    - |                - |    40.0391 |    11.7188 |        - |    496.85 KB |        1.00 |
|                 PLINQDefault |          4000 |     383.9 us |     7.59 us |     10.14 us |     384.4 us |  0.50 |    0.02 |              15.0000 |          11.6899 |    92.7734 |    69.3359 |        - |    1068.8 KB |        2.15 |
| EachChunkSeparatelyThenMerge |          4000 |     229.4 us |     5.02 us |     14.80 us |     226.1 us |  0.30 |    0.02 |               8.9709 |           0.0005 |    44.4336 |    19.0430 |        - |    542.67 KB |        1.09 |
|            CountByThenAssign |          4000 |     355.7 us |     2.03 us |      1.70 us |     355.3 us |  0.46 |    0.02 |              40.2339 |           0.0161 |    85.9375 |    16.6016 |        - |   1049.98 KB |        2.11 |
|   AtLestProjectionInParallel |          4000 |     238.5 us |     1.74 us |      1.54 us |     238.2 us |  0.31 |    0.01 |              17.2185 |           0.0083 |    58.1055 |    21.4844 |        - |    709.33 KB |        1.43 |
|         ConcurrentDictOfBags |          4000 |   1,065.4 us |    20.83 us |     37.57 us |   1,054.6 us |  1.40 |    0.07 |              30.6855 |          12.8301 |    78.1250 |    76.1719 |  19.5313 |    945.84 KB |        1.90 |
|                              |               |              |             |              |              |       |         |                      |                  |            |            |          |              |             |
|                   Sequential |        100000 |  20,260.2 us |   398.11 us |    822.17 us |  20,402.0 us |  1.00 |    0.00 |                    - |                - |  1031.2500 |   968.7500 |  31.2500 |  12676.32 KB |        1.00 |
|                 PLINQDefault |        100000 |   9,432.4 us |   145.78 us |    136.36 us |   9,372.9 us |  0.47 |    0.02 |              15.0000 |          11.5313 |  1593.7500 |   984.3750 | 218.7500 |  15491.71 KB |        1.22 |
| EachChunkSeparatelyThenMerge |        100000 |   5,797.0 us |   114.72 us |    132.11 us |   5,800.9 us |  0.29 |    0.01 |               9.0000 |                - |  1070.3125 |   968.7500 |  23.4375 |  12605.17 KB |        0.99 |
|            CountByThenAssign |        100000 |   8,492.9 us |   137.95 us |    129.04 us |   8,491.3 us |  0.42 |    0.02 |              49.7813 |           0.0625 |  2171.8750 |   312.5000 |  46.8750 |  25696.76 KB |        2.03 |
|   AtLestProjectionInParallel |        100000 |  11,697.1 us |   221.98 us |    264.25 us |  11,653.2 us |  0.58 |    0.02 |              23.1406 |           0.0156 |  1921.8750 |  1312.5000 | 671.8750 |  18387.66 KB |        1.45 |
|         ConcurrentDictOfBags |        100000 |  19,514.6 us |   217.25 us |    203.21 us |  19,577.6 us |  0.97 |    0.04 |              32.6875 |           1.5313 |  1718.7500 |   562.5000 | 156.2500 |  18793.39 KB |        1.48 |
|                              |               |              |             |              |              |       |         |                      |                  |            |            |          |              |             |
|                   Sequential |       2500000 | 489,129.5 us | 9,667.35 us | 18,157.62 us | 493,899.8 us |  1.00 |    0.00 |                    - |                - | 20000.0000 |  2000.0000 |        - | 312221.98 KB |        1.00 |
|                 PLINQDefault |       2500000 | 154,223.0 us | 2,219.31 us |  1,967.36 us | 154,692.9 us |  0.31 |    0.02 |              15.0000 |          11.0000 | 24000.0000 |  5000.0000 |        - | 369249.23 KB |        1.18 |
| EachChunkSeparatelyThenMerge |       2500000 | 131,198.2 us | 2,567.27 us |  3,598.95 us | 130,675.2 us |  0.27 |    0.01 |              10.2000 |                - | 21400.0000 |   800.0000 | 800.0000 |  332881.5 KB |        1.07 |
|            CountByThenAssign |       2500000 | 190,504.8 us | 1,572.94 us |  1,313.47 us | 190,323.7 us |  0.39 |    0.02 |              78.3333 |           0.3333 | 54000.0000 |          - |        - | 679230.84 KB |        2.18 |
|   AtLestProjectionInParallel |       2500000 | 176,499.6 us | 3,613.91 us | 10,541.94 us | 172,896.1 us |  0.37 |    0.03 |              56.6667 |                - | 21333.3333 |  1000.0000 | 333.3333 | 431321.01 KB |        1.38 |
|         ConcurrentDictOfBags |       2500000 | 429,738.8 us | 5,864.77 us |  5,485.91 us | 430,684.6 us |  0.87 |    0.04 |              45.0000 |           5.0000 | 38000.0000 | 10000.0000 |        - | 484466.88 KB |        1.55 |




## Reference record many buckets Easy projection, many buckets created - high overhead for constructing final arrays
```fsharp
type ReferenceRecordManyBuckets = {Id : int; Value : float}
    with interface IBenchMarkElement<ReferenceRecordManyBuckets> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> (x.Value.GetHashCode() * x.Id.GetHashCode() ) % 10_000
                
```

|                       Method | NumberOfItems |           Mean |         Error |        StdDev | Ratio | RatioSD | Completed Work Items | Lock Contentions |       Gen0 |       Gen1 |      Gen2 |      Allocated | Alloc Ratio |
|----------------------------- |-------------- |---------------:|--------------:|--------------:|------:|--------:|---------------------:|-----------------:|-----------:|-----------:|----------:|---------------:|------------:|
|                   Sequential |          4000 |       521.3 us |       9.91 us |      19.56 us |  1.00 |    0.00 |                    - |                - |    60.5469 |    59.5703 |   30.2734 |      757.57 KB |        1.00 |
|                 PLINQDefault |          4000 |       447.2 us |       6.13 us |       5.43 us |  0.86 |    0.04 |              15.0000 |          11.3857 |   156.7383 |   111.3281 |         - |     1851.95 KB |        2.44 |
| EachChunkSeparatelyThenMerge |          4000 |     1,840.0 us |      37.68 us |     110.52 us |  3.62 |    0.23 |               3.2910 |                - |   232.4219 |   230.4688 |   91.7969 |     2628.38 KB |        3.47 |
|            CountByThenAssign |          4000 |     1,251.6 us |      24.02 us |      29.50 us |  2.41 |    0.10 |              35.7480 |          50.1348 |   119.1406 |    58.5938 |   29.2969 |     1267.15 KB |        1.67 |
|   AtLestProjectionInParallel |          4000 |     1,096.9 us |      21.52 us |      39.88 us |  2.11 |    0.11 |               5.6973 |                - |   115.2344 |    85.9375 |   29.2969 |     1214.06 KB |        1.60 |
|         ConcurrentDictOfBags |          4000 |    15,995.8 us |     539.09 us |   1,589.52 us | 28.43 |    2.61 |              88.3438 |         309.8750 |   343.7500 |   328.1250 |   78.1250 |     3310.49 KB |        4.37 |
|                              |               |                |               |               |       |         |                      |                  |            |            |           |                |             |
|                   Sequential |        100000 |    14,487.8 us |     238.58 us |     211.49 us |  1.00 |    0.00 |                    - |                - |   890.6250 |   875.0000 |  484.3750 |     6872.23 KB |        1.00 |
|                 PLINQDefault |        100000 |    18,211.5 us |     363.23 us |     520.93 us |  1.25 |    0.03 |              15.0000 |           9.4063 |  1625.0000 |  1343.7500 |  375.0000 |    14418.94 KB |        2.10 |
| EachChunkSeparatelyThenMerge |        100000 |   415,792.4 us |  10,326.28 us |  30,447.28 us | 25.86 |    2.05 |               9.0000 |                - | 45000.0000 | 44000.0000 | 1000.0000 |   522716.23 KB |       76.06 |
|            CountByThenAssign |        100000 |    21,254.9 us |     414.68 us |     524.44 us |  1.46 |    0.04 |              41.8125 |          97.6250 |  1312.5000 |   875.0000 |  500.0000 |    12679.45 KB |        1.85 |
|   AtLestProjectionInParallel |        100000 |    19,238.0 us |     382.85 us |     618.23 us |  1.35 |    0.04 |              20.7188 |                - |  1718.7500 |  1687.5000 |  968.7500 |    12850.09 KB |        1.87 |
|         ConcurrentDictOfBags |        100000 |   145,946.7 us |   3,639.82 us |  10,559.78 us |  9.59 |    0.69 |              48.2500 |         715.2500 |  4000.0000 |  3750.0000 |  250.0000 |    47080.38 KB |        6.85 |
|                              |               |                |               |               |       |         |                      |                  |            |            |           |                |             |
|                   Sequential |       2500000 |   200,261.7 us |   3,867.45 us |   4,138.13 us |  1.00 |    0.00 |                    - |                - |  6666.6667 |  6333.3333 |  333.3333 |    79754.78 KB |        1.00 |
|                 PLINQDefault |       2500000 |   135,278.4 us |   2,354.65 us |   1,966.24 us |  0.68 |    0.02 |              15.0000 |           5.0000 | 12000.0000 |  9000.0000 | 1000.0000 |   158140.32 KB |        1.98 |
| EachChunkSeparatelyThenMerge |       2500000 | 2,212,316.3 us |  42,758.62 us |  37,904.40 us | 11.07 |    0.27 |              16.0000 |                - |  6000.0000 |  5000.0000 | 4000.0000 | 30082205.16 KB |      377.18 |
|            CountByThenAssign |       2500000 |    85,238.7 us |   1,640.00 us |   1,754.78 us |  0.43 |    0.01 |              60.8333 |         106.6667 | 15000.0000 |  3000.0000 |  166.6667 |   181514.74 KB |        2.28 |
|   AtLestProjectionInParallel |       2500000 |   404,878.5 us |   8,090.68 us |  19,998.17 us |  2.03 |    0.17 |              34.0000 |                - | 15000.0000 | 14000.0000 | 1000.0000 |   212708.16 KB |        2.67 |
|         ConcurrentDictOfBags |       2500000 | 1,878,675.7 us | 120,924.87 us | 356,549.70 us |  6.67 |    1.18 |             294.0000 |        3840.0000 | 49000.0000 | 47000.0000 | 1000.0000 |   575914.65 KB |        7.22 |


## Struct record - cheapest projection, low number of buckets. 
```fsharp
[<Struct>]
type StructRecord = {Id : int; Value : float}
    with interface IBenchMarkElement<StructRecord> 
            with 
                static member Create(id,value) = {Id = id; Value = value}
                static member Projection() = fun x -> x.Id % 20
                
```

|                       Method | NumberOfItems |         Mean |        Error |       StdDev | Ratio | RatioSD | Completed Work Items | Lock Contentions |       Gen0 |      Gen1 |      Gen2 |    Allocated | Alloc Ratio |
|----------------------------- |-------------- |-------------:|-------------:|-------------:|------:|--------:|---------------------:|-----------------:|-----------:|----------:|----------:|-------------:|------------:|
|                   Sequential |          4000 |     48.15 us |     0.946 us |     1.954 us |  1.00 |    0.00 |                    - |                - |    18.6157 |    7.2632 |         - |    228.49 KB |        1.00 |
|                 PLINQDefault |          4000 |    242.28 us |     3.398 us |     3.013 us |  5.08 |    0.22 |              15.0000 |          11.4214 |    89.8438 |   78.1250 |         - |   1050.04 KB |        4.60 |
| EachChunkSeparatelyThenMerge |          4000 |     30.46 us |     0.594 us |     0.730 us |  0.63 |    0.03 |               3.8105 |                - |    15.3809 |    7.5073 |         - |    186.26 KB |        0.82 |
|            CountByThenAssign |          4000 |     99.84 us |     1.097 us |     0.972 us |  2.09 |    0.09 |              37.1765 |           0.0045 |    28.0762 |    8.0566 |         - |    330.99 KB |        1.45 |
|   AtLestProjectionInParallel |          4000 |    164.80 us |     3.246 us |     3.986 us |  3.43 |    0.17 |              11.3196 |           0.0044 |    60.5469 |   30.2734 |   30.2734 |    503.16 KB |        2.20 |
|         ConcurrentDictOfBags |          4000 |    270.53 us |     5.346 us |     8.933 us |  5.59 |    0.31 |              23.4478 |           5.5166 |    44.9219 |   44.4336 |    9.2773 |    529.11 KB |        2.32 |
|                              |               |              |              |              |       |         |                      |                  |            |           |           |              |             |
|                   Sequential |        100000 |  1,540.41 us |    24.213 us |    22.649 us |  1.00 |    0.00 |                    - |                - |   906.2500 |  904.2969 |  570.3125 |   6690.98 KB |        1.00 |
|                 PLINQDefault |        100000 |  2,146.37 us |    42.405 us |    48.834 us |  1.40 |    0.03 |              15.0000 |          12.6914 |   929.6875 |  925.7813 |  277.3438 |   9850.91 KB |        1.47 |
| EachChunkSeparatelyThenMerge |        100000 |    624.56 us |     9.512 us |     8.432 us |  0.41 |    0.01 |               8.2529 |                - |   395.5078 |  394.5313 |         - |   3994.56 KB |        0.60 |
|            CountByThenAssign |        100000 |  1,978.30 us |    10.971 us |    10.263 us |  1.28 |    0.02 |              43.6367 |           0.0078 |   648.4375 |  644.5313 |         - |   7831.91 KB |        1.17 |
|   AtLestProjectionInParallel |        100000 |  4,407.41 us |    88.059 us |   177.883 us |  2.84 |    0.16 |              18.9063 |           0.0156 |  1226.5625 | 1218.7500 |  929.6875 |  13949.69 KB |        2.08 |
|         ConcurrentDictOfBags |        100000 |  4,882.83 us |    97.162 us |   240.160 us |  3.13 |    0.15 |              31.6250 |           3.7500 |  1476.5625 |  804.6875 |  265.6250 |  12331.27 KB |        1.84 |
|                              |               |              |              |              |       |         |                      |                  |            |           |           |              |             |
|                   Sequential |       2500000 | 38,750.17 us |   765.026 us |   818.569 us |  1.00 |    0.00 |                    - |                - |   615.3846 |  538.4615 |  461.5385 | 120992.89 KB |        1.00 |
|                 PLINQDefault |       2500000 | 42,599.69 us |   838.421 us | 1,768.514 us |  1.10 |    0.05 |              15.0000 |           9.8333 |  3833.3333 | 3750.0000 | 1083.3333 | 181891.79 KB |        1.50 |
| EachChunkSeparatelyThenMerge |       2500000 | 17,237.06 us |   521.317 us | 1,512.436 us |  0.43 |    0.03 |               8.9844 |                - |   406.2500 |  406.2500 |  406.2500 |  99194.59 KB |        0.82 |
|            CountByThenAssign |       2500000 | 52,086.16 us |   600.779 us |   561.969 us |  1.34 |    0.03 |              51.0000 |                - | 13000.0000 |  200.0000 |  200.0000 | 195339.72 KB |        1.61 |
|   AtLestProjectionInParallel |       2500000 | 69,371.65 us | 1,372.665 us | 3,367.172 us |  1.82 |    0.07 |              31.0000 |                - |  1285.7143 | 1142.8571 | 1142.8571 | 279168.97 KB |        2.31 |
|         ConcurrentDictOfBags |       2500000 | 50,787.33 us | 1,006.471 us | 2,122.989 us |  1.34 |    0.06 |              34.8000 |           0.2000 | 19300.0000 | 7400.0000 | 2000.0000 | 306187.28 KB |        2.53 |







All run using following setup:
BenchmarkDotNet=v0.13.4, OS=Windows 11 (10.0.22621.1265)
11th Gen Intel Core i9-11950H 2.60GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK=7.0.200
  [Host]     : .NET 7.0.3 (7.0.323.6910), X64 RyuJIT AVX2 DEBUG
  DefaultJob : .NET 7.0.3 (7.0.323.6910), X64 RyuJIT AVX2
