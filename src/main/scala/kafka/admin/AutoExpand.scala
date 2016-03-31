package kafka.admin

import joptsimple.OptionParser
import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.utils._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils
import collection._



object AutoExpandCommand extends Logging {
  def main(args: Array[String]): Unit = {
    val opts = new AutoExpandCommandOptions(args)
    val me = opts.options.valueOf(opts.selfBroker)
    val (addr,port) = me.split(':') match {
      case Array(e1,e2) =>
        (e1,Integer.parseInt(e2))
      case _ =>
        ("127.0.0.1",9092)
    }
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    val newBrokers = opts.options.valueOf(opts.up_down) match{
      case "up"=>
        zkUtils.getAllBrokersInCluster()
      case "down"=>
        zkUtils.getAllBrokersInCluster().filter{
          b=>
            b.getBrokerEndPoint(SecurityProtocol.PLAINTEXT) match{
              case BrokerEndPoint(_,a,p) if a==addr && p==port=>
                println("Exclude broker: %d".format(b.id))
                false
              case BrokerEndPoint(id,a,p)=>
                println("Broker: %d(%s:%d)".format(id,a,p))
                true
            }
        }
    }
    val newBrokersIds = newBrokers.map{
      b =>
        b.id
    }
    if (newBrokersIds.size<1){
      println("No broker found")
      return
    }
    val topics = zkUtils.getAllTopics()
    if (topics.size<1){
      println("No topics found")
      return
    }
    val topicPartitionsToReassign = zkUtils.getReplicaAssignmentForTopics(topics)

    var partitionsToBeReassigned : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val groupedByTopic = topicPartitionsToReassign.groupBy(tp => tp._1.topic)
    groupedByTopic.foreach { topicInfo =>
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(newBrokersIds, topicInfo._2.size,
        topicInfo._2.head._2.size)
      partitionsToBeReassigned ++= assignedReplicas.map(replicaInfo => (TopicAndPartition(topicInfo._1, replicaInfo._1) -> replicaInfo._2))
    }
    val currentPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(partitionsToBeReassigned.map(_._1.topic).toSeq)
    println("Current partition replica assignment\n\n%s"
      .format(zkUtils.getPartitionReassignmentZkData(currentPartitionReplicaAssignment)))
    println("Proposed partition reassignment configuration\n\n%s".format(zkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned)))
    val exec = new ReassignPartitionsCommand(zkUtils,partitionsToBeReassigned)
   // exec.reassignPartitions()
    if (exec.reassignPartitions()){
      var inProgress = true
      val start = System.currentTimeMillis()
      var stop = start
      var attemt = 1
      while (inProgress && (stop-start)<300000) {
        Thread.sleep(10000)
        println("Check reassigment attempt - %d".format(attemt))
        val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkUtils, partitionsToBeReassigned)
        partitionsToBeReassigned = partitionsToBeReassigned.filterKeys {
          partition =>
            reassignedPartitionsStatus.getOrElse(partition, ReassignmentInProgress) match {
              case ReassignmentCompleted =>
                println("Reassignment of partition %s completed successfully".format(partition))
                false
              case ReassignmentFailed =>
                println("Reassignment of partition %s failed".format(partition))
                false
              case ReassignmentInProgress =>
                println("Reassignment of partition %s is still in progress".format(partition))
                true
            }
        }
        inProgress = !partitionsToBeReassigned.isEmpty
        stop = System.currentTimeMillis()
        attemt+=1
      }
      if (inProgress){
        println("Timeout reassignment partitions:\n\n%s".format(zkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned)))
      } else{
        println("Reassignment compleated!!!")
      }
    } else{
      error("Invalid reassignment data. Emty partitions found!!!")
    }
    println("Rebalancing DONE!!!")

  }
  private def checkIfReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkUtils,topicAndPartition._1,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }
}

class AutoExpandCommandOptions(args: Array[String]) {
  val parser = new OptionParser

  val zkConnectOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the " +
    "form host:port. Multiple URLS can be given to allow fail-over.")
    .withOptionalArg()
    .defaultsTo("localhost:2181")
    .describedAs("urls")
    .ofType(classOf[String])

  val selfBroker = parser.accepts("broker", "Self broker addr")
  .withOptionalArg().defaultsTo("").ofType(classOf[String])

  val up_down = parser.accepts("updown", "Up or Down hook")
    .withOptionalArg()
    .defaultsTo("up")
    .ofType(classOf[String])

  val options = parser.parse(args: _*)

}