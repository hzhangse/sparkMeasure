package com.kyligence.sparkmeasure


import com.kyligence.promremoteclient._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.slf4j.{Logger, LoggerFactory}

/**
 * PrometheusSink: write Spark metrics and application info in near real-time to Prometheus
 *  use this mode to monitor Spark execution workload
 *  use for Grafana dashboard and analytics of job execution
 *  How to use: attach the PrometheusSInk to a Spark Context using the extra listener infrastructure.
 *  Example:
 *  --conf spark.extraListeners=com.kyligence.sparkmeasure.PrometheusSink
 *
 *  Configuration for PrometheusSink is handled with Spark conf parameters:
 *
 *  spark.sparkmeasure.prometheusURL = Prometheus endpoint URL
 *     example: --conf spark.sparkmeasure.prometheusURL="http://10.1.2.61:30003/api/v1/write"
 *
 *  spark.sparkmeasure.prometheusStagemetrics, boolean, default is false
 *
 * PrometheusSinkExtended: provides additional and verbose info on Task execution
 *  use: --conf spark.extraListeners=com.kyligence.sparkmeasure.PrometheusSinkExtended
 *
 * PrometheusSink: the amount of data generated is relatively small in most applications: O(number_of_stages)
 * PrometheusSinkExtended can generate a large amount of data O(Number_of_tasks), use with care
 */
class PrometheusSink(conf: SparkConf) extends SparkListener {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.warn("Custom monitoring listener with Prometheus sink initializing. Now attempting to connect to Prometheus")
  val url = parsePrometheusURL(conf, logger)
  val client = new Client(url)

  var appId = "noAppId"
  val logStageMetrics = parsePrometheusStagemetrics(conf, logger)

  appId = SparkSession.getActiveSession match {
    case Some(sparkSession) => sparkSession.sparkContext.applicationId
    case _ => "noAppId"
  }

  def parsePrometheusURL(conf: SparkConf, logger: Logger) : String = {
    // handle Prometheus URL
    val prometheusURL = conf.get("spark.sparkmeasure.prometheusURL", "http://10.1.2.61:30003/api/v1/write")
    if (prometheusURL.isEmpty) {
      logger.error("Prometheus URL not found, this will make the listener fail.")
      throw new RuntimeException
    } else {
      logger.info(s"Found URL for Prometheus: $prometheusURL")
    }
    prometheusURL
  }

  def parsePrometheusStagemetrics(conf: SparkConf, logger: Logger) : Boolean = {
    // handle Prometheus username and password
    val prometheusStagemetrics = conf.getBoolean("spark.sparkmeasure.prometheusStagemetrics", false)
    logger.info(s"Log also stagemetrics: ${prometheusStagemetrics.toString}")
    prometheusStagemetrics
  }

  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
    taskLocality match {
      case TaskLocality.PROCESS_LOCAL => 0
      case TaskLocality.NODE_LOCAL => 1
      case TaskLocality.RACK_LOCAL => 2
      case TaskLocality.NO_PREF => 3
      case TaskLocality.ANY => 4
      case _ => -1 // Flag an unknown situation
    }
  }
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorId = executorAdded.executorId
    val executorInfo = executorAdded.executorInfo
    val startTime = executorAdded.time

    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("executors_started").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorId").setValue(executorId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorHost").setValue(executorInfo.executorHost).build)
      .addLabels(Prometheus.Label.newBuilder.setName("totalCores").setValue(executorInfo.totalCores.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("startTime").setValue(startTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(startTime).setTimestamp(startTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0L)
    val attemptNumber = stageSubmitted.stageInfo.attemptNumber()
    val stageId = stageSubmitted.stageInfo.stageId

    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("stages_started").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(stageId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("attemptNUmber").setValue(attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("submissionTime").setValue(submissionTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(submissionTime).setTimestamp(submissionTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val submissionTime = stageCompleted.stageInfo.submissionTime.getOrElse(0L)
    val completionTime = stageCompleted.stageInfo.completionTime.getOrElse(0L)
    val attemptNumber = stageCompleted.stageInfo.attemptNumber()


    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("stages_ended").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(stageId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("attemptNUmber").setValue(attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("submissionTime").setValue(submissionTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("completionTime").setValue(completionTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(completionTime).setTimestamp(completionTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)

    if (logStageMetrics) {
      val taskmetrics = stageCompleted.stageInfo.taskMetrics

      var timeSeries1 = Prometheus.TimeSeries.newBuilder()
        .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("stages_ended").build)
        .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
        .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(stageId.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("attemptNUmber").setValue(attemptNumber.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("failureReason").setValue(stageCompleted.stageInfo.failureReason.getOrElse("")).build)
        .addLabels(Prometheus.Label.newBuilder.setName("submissionTime").setValue(submissionTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("completionTime").setValue(completionTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("executorRunTime").setValue(taskmetrics.executorRunTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("executorCpuTime").setValue(taskmetrics.executorCpuTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("executorDeserializeCpuTime").setValue(taskmetrics.executorDeserializeCpuTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("executorDeserializeTime").setValue(taskmetrics.executorDeserializeTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("jvmGCTime").setValue(taskmetrics.jvmGCTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("memoryBytesSpilled").setValue(taskmetrics.memoryBytesSpilled.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("peakExecutionMemory").setValue(taskmetrics.peakExecutionMemory.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("resultSerializationTime").setValue(taskmetrics.resultSerializationTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("resultSize").setValue(taskmetrics.resultSize.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("bytesRead").setValue(taskmetrics.inputMetrics.bytesRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("recordsRead").setValue(taskmetrics.inputMetrics.recordsRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("bytesWritten").setValue(taskmetrics.outputMetrics.bytesWritten.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("recordsWritten").setValue(taskmetrics.outputMetrics.recordsWritten.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleTotalBytesRead").setValue(taskmetrics.shuffleReadMetrics.totalBytesRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleRemoteBytesRead").setValue(taskmetrics.shuffleReadMetrics.remoteBytesRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleRemoteBytesReadToDisk").setValue(taskmetrics.shuffleReadMetrics.remoteBytesReadToDisk.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleLocalBytesRead").setValue(taskmetrics.shuffleReadMetrics.localBytesRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleTotalBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.totalBlocksFetched.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleLocalBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.localBlocksFetched.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleRemoteBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.remoteBlocksFetched.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleRecordsRead").setValue(taskmetrics.shuffleReadMetrics.recordsRead.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleFetchWaitTime").setValue(taskmetrics.shuffleReadMetrics.fetchWaitTime.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleBytesWritten").setValue(taskmetrics.shuffleWriteMetrics.bytesWritten.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleRecordsWritten").setValue(taskmetrics.shuffleWriteMetrics.recordsWritten.toString).build)
        .addLabels(Prometheus.Label.newBuilder.setName("shuffleWriteTime").setValue(taskmetrics.shuffleWriteMetrics.writeTime.toString).build)
        .addSamples(Prometheus.Sample.newBuilder.setValue(completionTime).setTimestamp(completionTime).build).build()
      val req1 = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries1).build()
      client.WriteProto(req1)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => {
        val startTime = e.time
        val queryId = e.executionId
        val description = e.description
        // val details = e.details

        var timeSeries = Prometheus.TimeSeries.newBuilder()
          .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("queries_started").build)
          .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
          .addLabels(Prometheus.Label.newBuilder.setName("description").setValue(description).build)
          .addLabels(Prometheus.Label.newBuilder.setName("queryId").setValue(queryId.toString).build)
          .addLabels(Prometheus.Label.newBuilder.setName("startTime").setValue(startTime.toString).build)
          .addSamples(Prometheus.Sample.newBuilder.setValue(startTime).setTimestamp(startTime).build).build()
        val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
        client.WriteProto(req)

      }
      case e: SparkListenerSQLExecutionEnd => {
        val endTime = e.time
        val queryId = e.executionId

        var timeSeries = Prometheus.TimeSeries.newBuilder()
          .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("queries_ended").build)
          .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
          .addLabels(Prometheus.Label.newBuilder.setName("queryId").setValue(queryId.toString).build)
          .addLabels(Prometheus.Label.newBuilder.setName("endTime").setValue(endTime.toString).build)
          .addSamples(Prometheus.Sample.newBuilder.setValue(endTime).setTimestamp(endTime).build).build()
        val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
        client.WriteProto(req)
      }
      case _ => None // Ignore
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startTime = jobStart.time
    val jobId = jobStart.jobId
    var timeSeries = Prometheus.TimeSeries.newBuilder()
    .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("jobs_started").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("jobID").setValue(jobId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("startTime").setValue(startTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(startTime).setTimestamp(startTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val completionTime = jobEnd.time
    val jobId = jobEnd.jobId

    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("jobs_ended").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("jobID").setValue(jobId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("completionTime").setValue(completionTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(completionTime).setTimestamp(completionTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appId = applicationStart.appId.getOrElse("noAppId")
    // val appName = applicationStart.appName
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}, closing Prometheus connection.")
  }

}

/**
 * PrometheusSinkExtended extends the basic PrometheusSink Sink functionality with a verbose dump of
 * Task metrics and task info into Prometheus
 * Note: this can generate a large amount of data O(Number_of_tasks)
 * Configuration parameters and how-to use: see PrometheusSink
 */
class PrometheusSinkExtended(conf: SparkConf) extends PrometheusSink(conf: SparkConf) {

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("tasks_started").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("taskId").setValue(taskInfo.taskId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("attemptNumber").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("launchTime").setValue(taskInfo.launchTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(taskInfo.launchTime).setTimestamp(taskInfo.launchTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskmetrics = taskEnd.taskMetrics

    var timeSeries = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("tasks_ended").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("taskId").setValue(taskInfo.taskId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("attemptNumber").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("launchTime").setValue(taskInfo.launchTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("finishTime").setValue(taskInfo.finishTime.toString).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(taskInfo.finishTime).setTimestamp(taskInfo.finishTime).build).build()
    val req = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries).build()
    client.WriteProto(req)


    var timeSeries1 = Prometheus.TimeSeries.newBuilder()
      .addLabels(Prometheus.Label.newBuilder.setName("__name__").setValue("task_metrics").build)
      .addLabels(Prometheus.Label.newBuilder.setName("applicationId").setValue(appId).build)
      .addSamples(Prometheus.Sample.newBuilder.setValue(taskInfo.finishTime).setTimestamp(taskInfo.finishTime).build)
      // task info
      .addLabels(Prometheus.Label.newBuilder.setName("taskId").setValue(taskInfo.taskId.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("attemptNumber").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("stageId").setValue(taskInfo.attemptNumber.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("launchTime").setValue(taskInfo.launchTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("completionTime").setValue(taskInfo.finishTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("failed").setValue(taskInfo.failed.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("speculative").setValue(taskInfo.speculative.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("killed").setValue(taskInfo.killed.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("finished").setValue(taskInfo.finished.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorId").setValue(taskInfo.executorId).build)
      .addLabels(Prometheus.Label.newBuilder.setName("duration").setValue(taskInfo.duration.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("successful").setValue(taskInfo.successful.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("host").setValue(taskInfo.host).build)
      .addLabels(Prometheus.Label.newBuilder.setName("taskLocality").setValue(encodeTaskLocality(taskInfo.taskLocality).toString).build)
      // task metrics
      .addLabels(Prometheus.Label.newBuilder.setName("executorRunTime").setValue(taskmetrics.executorRunTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorCpuTime").setValue(taskmetrics.executorCpuTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorDeserializeCpuTime").setValue(taskmetrics.executorDeserializeCpuTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("executorDeserializeTime").setValue(taskmetrics.executorDeserializeTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("jvmGCTime").setValue(taskmetrics.jvmGCTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("memoryBytesSpilled").setValue(taskmetrics.memoryBytesSpilled.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("peakExecutionMemory").setValue(taskmetrics.peakExecutionMemory.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("resultSerializationTime").setValue(taskmetrics.resultSerializationTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("resultSize").setValue(taskmetrics.resultSize.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("bytesRead").setValue(taskmetrics.inputMetrics.recordsRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("recordsRead").setValue(taskmetrics.inputMetrics.recordsRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("bytesWritten").setValue(taskmetrics.outputMetrics.bytesWritten.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("recordsWritten").setValue(taskmetrics.outputMetrics.recordsWritten.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleTotalBytesRead").setValue(taskmetrics.shuffleReadMetrics.totalBytesRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleRemoteBytesRead").setValue(taskmetrics.shuffleReadMetrics.remoteBytesRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleLocalBytesRead").setValue(taskmetrics.shuffleReadMetrics.localBytesRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleTotalBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.localBlocksFetched.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleLocalBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.totalBytesRead.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleRemoteBlocksFetched").setValue(taskmetrics.shuffleReadMetrics.remoteBlocksFetched.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleRecordsRead").setValue(taskmetrics.shuffleReadMetrics.recordsRead.toString).build)
      // this requires spark2.3 and above .addField("remoteBytesReadToDisk", taskmetrics.shuffleReadMetrics.remoteBytesReadToDisk)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleFetchWaitTime").setValue(taskmetrics.shuffleReadMetrics.fetchWaitTime.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleBytesWritten").setValue(taskmetrics.shuffleWriteMetrics.bytesWritten.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleRecordsWritten").setValue(taskmetrics.shuffleWriteMetrics.recordsWritten.toString).build)
      .addLabels(Prometheus.Label.newBuilder.setName("shuffleWriteTime").setValue(taskmetrics.shuffleWriteMetrics.writeTime.toString).build)
      .build()
    val req1 = Prometheus.WriteRequest.newBuilder.addTimeseries(timeSeries1).build()
    client.WriteProto(req1)
  }
}
