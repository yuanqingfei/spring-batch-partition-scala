package com.yuanqingfei.batch

import java.sql.{ResultSet, SQLException}
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSource
import com.yuanqingfei.batch.pojo.{OfbizProduct, RedisProduct}
import org.apache.log4j.Logger
import org.springframework.batch.core.configuration.annotation._
import org.springframework.batch.core.partition.PartitionHandler
import org.springframework.batch.core.partition.support.{Partitioner, TaskExecutorPartitionHandler}
import org.springframework.batch.core.{Job, Step}
import org.springframework.batch.item.{ItemProcessor, ItemReader, ItemWriter}
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.beans.factory.annotation.{Autowired, Qualifier, Value}
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Primary, PropertySource}
import org.springframework.core.task.TaskExecutor
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory
import org.springframework.data.redis.core.{RedisTemplate, StringRedisTemplate}
import org.springframework.jdbc.core.{ArgumentPreparedStatementSetter, RowMapper}
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

/**
  * Created by aaron on 16-5-3.
  */

@SpringBootApplication
@EnableBatchProcessing
@PropertySource(Array("classpath:jdbc.properties", "classpath:redis.properties"))
class MyBatchApplication {

  val logger: Logger = Logger.getLogger(classOf[MyBatchApplication])

  @Value("${jdbc.url}")
  var jdbcUrl: String = _

  @Value("${jdbc.username}")
  var jdbcUserName: String = _

  @Value("${jdbc.password}")
  var jdbcPassword: String = _

  @Value("${redis.host}")
  var redisHost: String = _

  @Value("${redis.port}")
  var redisPort: String = _

  @Autowired
  private var jobs: JobBuilderFactory = _

  @Autowired
  private var steps: StepBuilderFactory = _

  @Bean
  def druidDataSource: DruidDataSource = {
    val ds: DruidDataSource = new DruidDataSource
    ds.setUrl(jdbcUrl)
    ds.setPassword(jdbcPassword)
    ds.setUsername(jdbcUserName)
    return ds
  }

  @Primary
  @Bean
  @ConfigurationProperties(prefix = "datasource.hsql")
  def dataSource: DataSource = {
    return DataSourceBuilder.create.build
  }

  @Bean
  def configurer(@Qualifier("dataSource") batchDataSource: DataSource): BatchConfigurer = {
    return new DefaultBatchConfigurer(batchDataSource)
  }

  @Bean
  def connectionFactory: RedisConnectionFactory = {
    val factory: JedisConnectionFactory = new JedisConnectionFactory
    factory.setHostName(redisHost)
    factory.setPort(redisPort.toInt)

    return factory
  }

  @Bean
  def stringTemplate(redisConnectionFactory: RedisConnectionFactory): StringRedisTemplate = {
    val stringTemplate: StringRedisTemplate = new StringRedisTemplate
    stringTemplate.setConnectionFactory(redisConnectionFactory)
    return stringTemplate
  }

  /////////////////////////////

  @Bean
  @StepScope
  def itemReader(@Value("#{stepExecutionContext['category']}") brandNo: String, @Qualifier("druidDataSource") ds: DataSource): JdbcCursorItemReader[OfbizProduct] = {
    System.out.println("BrandNo: " + brandNo)
    val itemReader: JdbcCursorItemReader[OfbizProduct] = new JdbcCursorItemReader[OfbizProduct]

    itemReader.setDataSource(ds)
    itemReader.setSql("select id, name, category from my_product where category = ?")
    itemReader.setRowMapper(new RowMapper[OfbizProduct]() {
      @throws[SQLException]
      def mapRow(rs: ResultSet, rowNum: Int): OfbizProduct = {
        val user: OfbizProduct = new OfbizProduct
        user.setId(rs.getString("id"))
        user.setName(rs.getString("name"))
        user.setCategory(rs.getString("category"))
        logger.info("USER: " + user)
        return user
      }
    })
    val setter: ArgumentPreparedStatementSetter = new ArgumentPreparedStatementSetter(Array(brandNo))
    itemReader.setPreparedStatementSetter(setter)
    return itemReader
  }

  @Bean
  def itemProcessor: ItemProcessor[OfbizProduct, RedisProduct] = {
    return new ItemProcessor[OfbizProduct, RedisProduct]() {
      @throws[Exception]
      override def process(item: OfbizProduct): RedisProduct = {
        val user: RedisProduct = new RedisProduct
        user.setKey(item.getId)
        user.setValue(item.getName + "_" + item.getCategory)
        return user
      }
    }
  }

  @Bean
  def itemWriter(template: StringRedisTemplate): ItemWriter[RedisProduct] = {
    return new ItemWriter[RedisProduct]() {
      @throws[Exception]
      override def write(items: java.util.List[_ <: RedisProduct]) {
        import scala.collection.JavaConversions._
        for (product <- items) {
          template.opsForValue.set(product.key, product.value)
        }
      }
    }
  }

  @Bean
  def readWritePartitionedStep(reader: ItemReader[OfbizProduct], processor: ItemProcessor[OfbizProduct, RedisProduct], writer: ItemWriter[RedisProduct]): Step = {
    return steps.get("readWritePartitionedStep")
      .chunk[OfbizProduct, RedisProduct](10)
      .reader(reader)
      .processor(processor)
      .writer(writer)
      .build
  }

  ////////////////////////

  @Bean
  def databaseReadingPartitioningJob(@Qualifier("partitionedStep") partitionedStep: Step): Job = {
    return jobs.get("databaseReadingPartitioningJob")
      .start(partitionedStep)
      .build
  }

  @Bean
  def partitionedStep(partitioner: Partitioner, handler: PartitionHandler): Step = {
    return steps.get("partitionedStep")
      .partitioner("readWritePartitionedStep_Slave", partitioner)
      .partitionHandler(handler)
      .build
  }

  @Bean
  def handler(@Qualifier("readWritePartitionedStep") readWritePartitionedStep: Step, taskExecutor: TaskExecutor): PartitionHandler = {
    val handler: TaskExecutorPartitionHandler = new TaskExecutorPartitionHandler
    handler.setTaskExecutor(taskExecutor)
    handler.setStep(readWritePartitionedStep)
    return handler
  }

  @Bean
  def taskExecutor: TaskExecutor = {
    val taskExecutor: ThreadPoolTaskExecutor = new ThreadPoolTaskExecutor
    taskExecutor.setCorePoolSize(10)
    return taskExecutor
  }

  @Bean
  def partitioner(@Qualifier("druidDataSource") ds: DataSource): Partitioner = {
    val brandPartitioner: CategoryPartitioner = new CategoryPartitioner(ds)
    return brandPartitioner
  }

}
