docker build -f docker/dockerfile -t registry.cn-shanghai.aliyuncs.com/kyligence/spark:v3.3-prom .
docker push registry.cn-shanghai.aliyuncs.com/kyligence/spark:v3.3-prom

/opt/spark/bin/spark-submit     --master k8s://https://10.1.2.63:6443     --deploy-mode cluster     --name hello-0    \
 --class "org.apache.spark.examples.SparkPi"     --driver-memory="500M"       --executor-memory="500M"      \
 --conf spark.plugins=ch.cern.CgroupMetrics     --conf spark.cernSparkPlugin.registerOnDriver=true     \
 --conf spark.extraListeners=com.kyligence.sparkmeasure.PrometheusSink     \
 --conf spark.sparkmeasure.prometheusURL="http://10.1.2.61:30003/api/v1/write"     \
 --conf spark.sparkmeasure.prometheusStagemetrics=true     --conf "spark.master=k8s://https://10.1.2.63:6443"    \
 --conf "spark.app.name=hello-0"     --conf "spark.driver.extraJavaOptions"="-Divy.cache.dir=/tmp -Divy.home=/tmp"    \
 --conf "spark.driver.extraClassPath=/opt/spark/plugins/*:/opt/spark/listeners/*:/opt/spark/listeners/lib/*"     \
 --conf "spark.executor.extraClassPath=/opt/spark/plugins/*"     --conf "spark.driver.memory=500M"     \
 --conf "spark.driver.memoryOverhead=512M"     --conf "spark.executor.memory=500M"     --conf "spark.executor.memoryOverhead=512M"     \
 --conf "spark.eventLog.enabled=false"      --conf "spark.kubernetes.container.image.pullPolicy=Always"     \
 --conf "spark.kubernetes.namespace=default"      --conf "spark.kubernetes.scheduler.name=volcano"      \
 --conf "spark.kubernetes.scheduler.volcano.podGroupTemplateFile=pg-hello-0.yaml"    \
 --conf "spark.kubernetes.driver.pod.featureSteps=org.apache.spark.deploy.k8s.features.VolcanoFeatureStep"
  --conf "spark.kubernetes.driver.label.app=hello-0"     --conf "spark.kubernetes.driver.request.cores=1"     --conf "spark.kubernetes.driver.limit.cores=1"     \
 --conf "spark.kubernetes.driver.secrets.apiserver=/opt/pki"     --conf "spark.kubernetes.executor.deleteOnTermination=true"
    --conf "spark.kubernetes.executor.request.cores=1"     --conf "spark.kubernetes.executor.limit.cores=1"
     --conf "spark.kubernetes.executor.podNamePrefix=hello-0"      --conf "spark.kubernetes.executor.scheduler.name=volcano"
        --conf "spark.kubernetes.executor.pod.featureSteps=org.apache.spark.deploy.k8s.features.VolcanoFeatureStep"
         --conf "spark.kubernetes.authenticate.driver.mounted.oauthTokenFile=/opt/pki/token"
          --conf "spark.kubernetes.authenticate.driver.caCertFile=/opt/pki/ca.crt"
           --conf "spark.kubernetes.authenticate.driver.serviceAccountName=my-release-spark"
             --conf "spark.kubernetes.authenticate.driver.oauthTokenFile=/opt/pki/token"
               --conf "spark.kubernetes.authenticate.executor.serviceAccountName= my-release-spark"
                --conf "spark.kubernetes.authenticate.caCertFile=/opt/pki/ca.crt"
                 --conf "spark.kubernetes.authenticate.oauthTokenFile=/opt/pki/token"
                   --conf "spark.dynamicAllocation.executorIdleTimeout=10s"
                   --conf "spark.dynamicAllocation.cachedExecutorIdleTimeout=200s"
                   --conf "spark.dynamicAllocation.minExecutors=1"     --conf "spark.dynamicAllocation.initialExecutors=1"
                   --conf "spark.dynamicAllocation.maxExecutors=2"      --conf "spark.dynamicAllocation.executorAllocationRatio=0.5"
                      --conf "spark.dynamicAllocation.enabled=true"     \
 --conf "spark.dynamicAllocation.shuffleTracking.enabled=true"
   --conf "spark.metrics.conf.*.source.jvm.class"="org.apache.spark.metrics.source.JvmSource"     \
 --conf spark.metrics.appStatusSource.enabled=true     \
 --conf spark.ui.prometheus.enabled=true \
 --conf spark.kubernetes.driver.annotation.prometheus.io/scrape=true \
 --conf spark.kubernetes.driver.annotation.prometheus.io/path=/metrics/executors/prometheus \
 --conf spark.kubernetes.driver.annotation.prometheus.io/port=4040 \
 --conf "spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet" \
 --conf "spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus" \
 --conf "spark.metrics.conf.applications.sink.prometheusServlet.path=/metrics/applications/prometheus" \
 --conf "spark.metrics.conf.*.sink.servlet.class=org.apache.spark.metrics.sink.MetricsServlet" \
 --conf "spark.metrics.conf.*.sink.servlet.path=/metrics/json" \
 --conf "spark.metrics.conf.master.sink.servlet.path=/metrics/master/json" \
 --conf "spark.metrics.conf.applications.sink.servlet.path=/metrics/applications/json" \
 --conf "spark.metrics.conf.*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink" \
 --conf "spark.driver.extraJavaOptions=-javaagent:/opt/spark/listeners/lib/jmx_prometheus_javaagent-0.17.0.jar=8080:/opt/spark/listeners/lib/spark-jmx.yml" \
 --conf "spark.kubernetes.container.image=registry.cn-shanghai.aliyuncs.com/kyligence/spark:v3.3-prom"     \
 local:///opt/spark/examples/jars/spark-examples_2.12-3.3.0-SNAPSHOT.jar 100000


 cat <<EOF | curl --data-binary @- http://10.1.2.63:30091/metrics/job/test_job/instance/test_instance
> #TYPE node_memory_usage gauge
> node_memory_usage 36
> # TYPE memory_total gauge
> node_memory_total 36000
> EOF
