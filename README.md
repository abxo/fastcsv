# fastcsv
A fast CSV multi-thread reader

主要特性：
- 可按列读取，减少不需要的对象生成，减少内存使用，提高处理速度
- 多线程处理，文件读线程、处理线程可灵活配置（处理线程数目前必须为2的幂）

示例：

        String file = ...
        String [] cols = ...
        double[][] data = CSV.readMatrix(file, cols, 4, 8, 0);
    
        String[][] d2 = CSV.readCsv(file, cols, true, 4, 1, 0);

        
性能：

        ![](fastcsv.PNG)
