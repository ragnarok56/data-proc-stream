package net.nacios.spark.listener

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class CustomQueryExecutionListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println("in onSuccess")
    println(s"$funcName - $qe")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println("in onFailure")
    println(s"$funcName - $qe")
  }
}