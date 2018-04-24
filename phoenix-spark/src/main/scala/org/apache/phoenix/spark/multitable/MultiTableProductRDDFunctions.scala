package org.apache.phoenix.spark.multitable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.spark.{ConfigurationUtil, PhoenixRecordWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._


class MultiTableProductRDDFunctions[V <: InternalRow](data: RDD[(String, V)]) extends Serializable {

  def saveToPhoenix(conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None,
                    tenantId: Option[String] = None): Unit = {

    val phxRDD = data.mapPartitions { rows =>
      rows.map { row =>
        val key = row._1
        val rowBody: InternalRow = row._2

        val tableName = Util.getTableName(key, conf)
        val structType: StructType = Util.getStructType(key, conf)
        val upsertColumns = Util.getUpsertColumns(key, conf)

        val columns = Util.getUpsertColumnMetadataList(tableName, upsertColumns, conf).toList

        val rec = new MultiTalbePhoenixRecordWritable(tableName, columns)

        rowBody.toSeq(structType).foreach { e => rec.add(e) }
        (null, rec)
      }
    }

    // Save it
    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[PhoenixRecordWritable],
      classOf[PhoenixOutputFormat[PhoenixRecordWritable]],
      conf
    )
  }
}