package com.yuanqingfei.batch

import javax.sql.DataSource

import org.apache.log4j.Logger
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ExecutionContext
import org.springframework.jdbc.core.{JdbcOperations, JdbcTemplate}

/**
  * Created by aaron on 16-5-8.
  */
class CategoryPartitioner(dataSource: DataSource) extends Partitioner{
  private val logger: Logger = Logger.getLogger(classOf[CategoryPartitioner])
  private val jdbcTemplate: JdbcOperations = new JdbcTemplate(dataSource)

  def partition(gridSize: Int): java.util.Map[String, ExecutionContext] = {
    logger.info("begin partition")
    val brandIds: java.util.List[String] = jdbcTemplate.queryForList("select distinct(category) from my_product;", classOf[String])
    val results: java.util.Map[String, ExecutionContext] = new java.util.LinkedHashMap[String, ExecutionContext]
    import scala.collection.JavaConversions._
    for (brandId <- brandIds) {
      val context: ExecutionContext = new ExecutionContext
      context.put("category", brandId)
      results.put("partition." + brandId, context)
    }
    return results
  }
}
