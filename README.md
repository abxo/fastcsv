# fastcsv
A fast CSV multi-thread reader

## 主要特性：
- 可按列读取，减少不需要的对象生成，减少内存使用，提高处理速度
- 多线程处理，文件读线程、处理线程可灵活配置（处理线程数目前必须为2的幂）

## 示例：

        String file = ...
        String [] cols = ...
        double[][] data = CSV.readMatrix(file, cols, 4, 8, 0);
    
        String[][] d2 = CSV.readCsv(file, cols, true, 4, 1, 0);

        
## 单线程纯解析性能
- （测试代码修改自![JavaCsvBenchmarkSuite](https://github.com/osiegmar/JavaCsvBenchmarkSuite)）：

![](fastcsv.PNG)



## 【测试说明】

       上述性能测试，不涉及磁盘、网络，仅仅为内存数据处理速度，并且不保存数据，跟真实读取有较大差距。
       
       真实数据测试（i7 4core 8thread, 12 GB内存）：
       300MB，每行482列，共50000行csv，读取String[][] 耗时0.3-1.5秒左右（受GC影响严重），读取double[][]耗时0.45-0.6秒
       多核服务器线程数足够时，性能会提升更加明显
      
       
## 最后

- 本开源代码移除了涉及GPL版权的部分，这部分代码主要包含减少内存使用，及处理加速
- JDK9以上版本的优化部分未体现


