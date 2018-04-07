B1: Tạo hai thư mục thangbk2209/fiveMinutes_6176858948 và thangbk2209/resource_fiveMinutes_6176858948
B2. ra ngoài thư mục spark2.7...(Thư mục cha của thangbk2209)
 chạy lệnh sudo ./bin/spark-submit thangbk2209/google_cluster_analysis/exTractResourceTopJobId.py
B3: Chạy sudo ./bin/spark-submit thangbk2209/google_cluster_analysis/sumResource.py
B4: Chạy sudo python thangbk2209/google_cluster_analysis/AddColumns.py
B5: Chạy sudo python thangbk2209/google_cluster_analysis/excuteNaN.py
B6: Chạy sudo python thangbk2209/google_cluster_analysis/sort_Sample_resource_data.py
Lệnh:
./bin/spark-submit thangbk2209/google_cluster_analysis/JobMaxTask.py --conf spark.driver.cores==16 spark.executor.memory == 16 
&& ./bin/spark-submit thangbk2209/google_cluster_analysis/minStart_maxEndPerJob.py --conf spark.driver.cores==16 spark.executor.memory == 16 
&& python thangbk2209/google_cluster_analysis/concat.py && ./bin/spark-submit thangbk2209/google_cluster_analysis/exTractResourceTopJobId.py --conf spark.driver.cores==16 spark.executor.memory == 16 && ./bin/spark-submit thangbk2209/google_cluster_analysis/sumResource.py --conf spark.driver.cores==16 spark.executor.memory == 16 && python thangbk2209/google_cluster_analysis/AddColumns.py && python thangbk2209/google_cluster_analysis/excuteNaN.py && ./bin/spark-submit thangbk2209/google_cluster_analysis/sort_Sample_resource_data.py