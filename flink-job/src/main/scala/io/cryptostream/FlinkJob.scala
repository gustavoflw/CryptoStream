package io.cryptostream

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

case class Trade(symbol: String, price: Double, volume: Double, timestamp: Long)

case class PriceAggregate(
  symbol: String,
  windowStart: Long,
  windowEnd: Long,
  avgPrice: Double,
  minPrice: Double,
  maxPrice: Double,
  totalVolume: Double,
  tradeCount: Long,
)

case class Alert(
  symbol: String,
  windowStart: Long,
  prevAvgPrice: Double,
  currAvgPrice: Double,
  changePercent: Double,
)

// Mutable POJO accumulator — no-arg constructor + var fields for Flink's type extractor.
class TradeAccum {
  var symbol: String       = ""
  var priceSum: Double     = 0.0
  var minPrice: Double     = Double.MaxValue
  var maxPrice: Double     = Double.MinValue
  var totalVolume: Double  = 0.0
  var tradeCount: Long     = 0L
  var firstTimestamp: Long = Long.MaxValue
  var lastTimestamp: Long  = Long.MinValue
}

// ---------------------------------------------------------------------------
// Windowed aggregation
// ---------------------------------------------------------------------------

class TradeAggregator extends AggregateFunction[Trade, TradeAccum, PriceAggregate] {
  override def createAccumulator(): TradeAccum = new TradeAccum

  override def add(t: Trade, acc: TradeAccum): TradeAccum = {
    acc.symbol         = t.symbol
    acc.priceSum      += t.price
    acc.minPrice       = math.min(acc.minPrice, t.price)
    acc.maxPrice       = math.max(acc.maxPrice, t.price)
    acc.totalVolume   += t.volume
    acc.tradeCount    += 1
    acc.firstTimestamp = math.min(acc.firstTimestamp, t.timestamp)
    acc.lastTimestamp  = math.max(acc.lastTimestamp, t.timestamp)
    acc
  }

  override def getResult(acc: TradeAccum): PriceAggregate =
    PriceAggregate(
      symbol      = acc.symbol,
      windowStart = acc.firstTimestamp,
      windowEnd   = acc.lastTimestamp,
      avgPrice    = if (acc.tradeCount > 0) acc.priceSum / acc.tradeCount else 0.0,
      minPrice    = acc.minPrice,
      maxPrice    = acc.maxPrice,
      totalVolume = acc.totalVolume,
      tradeCount  = acc.tradeCount,
    )

  override def merge(a: TradeAccum, b: TradeAccum): TradeAccum = {
    a.priceSum       += b.priceSum
    a.minPrice        = math.min(a.minPrice, b.minPrice)
    a.maxPrice        = math.max(a.maxPrice, b.maxPrice)
    a.totalVolume    += b.totalVolume
    a.tradeCount     += b.tradeCount
    a.firstTimestamp  = math.min(a.firstTimestamp, b.firstTimestamp)
    a.lastTimestamp   = math.max(a.lastTimestamp, b.lastTimestamp)
    a
  }
}

// ---------------------------------------------------------------------------
// Spike detection
// ---------------------------------------------------------------------------

class SpikeDetector(threshold: Double)
    extends RichFlatMapFunction[PriceAggregate, Alert] {
  @transient private var prevAvg: ValueState[java.lang.Double] = _

  override def open(config: Configuration): Unit =
    prevAvg = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Double]("prevAvg", classOf[java.lang.Double])
    )

  override def flatMap(agg: PriceAggregate, out: Collector[Alert]): Unit = {
    val prev = prevAvg.value()
    if (prev != null && prev != 0.0) {
      val change = math.abs((agg.avgPrice - prev) / prev * 100.0)
      if (change >= threshold)
        out.collect(Alert(agg.symbol, agg.windowStart, prev, agg.avgPrice, change))
    }
    prevAvg.update(agg.avgPrice)
  }
}

// ---------------------------------------------------------------------------
// Confluent wire-format Avro serializer for Kafka sinks
// ---------------------------------------------------------------------------

class ConfluentAvroSerializer(schemaStr: String, subject: String, registryUrl: String)
    extends SerializationSchema[GenericRecord] {

  @transient private var schemaId: Int                          = -1
  @transient private var writer: GenericDatumWriter[GenericRecord] = _

  override def open(ctx: SerializationSchema.InitializationContext): Unit = {
    val parsed = new Schema.Parser().parse(schemaStr)
    val client = new CachedSchemaRegistryClient(registryUrl, 100)
    schemaId = client.register(subject, new AvroSchema(parsed))
    writer   = new GenericDatumWriter[GenericRecord](parsed)
  }

  override def serialize(record: GenericRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(0)                                         // magic byte
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array()) // 4-byte schema ID
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }
}

// ---------------------------------------------------------------------------
// Case class → GenericRecord mappers
// ---------------------------------------------------------------------------

class PriceAggregateMapper(schemaStr: String) extends RichMapFunction[PriceAggregate, GenericRecord] {
  @transient private var schema: Schema = _

  override def open(config: Configuration): Unit =
    schema = new Schema.Parser().parse(schemaStr)

  override def map(a: PriceAggregate): GenericRecord = {
    val r = new GenericData.Record(schema)
    r.put("symbol",      a.symbol)
    r.put("windowStart", a.windowStart)
    r.put("windowEnd",   a.windowEnd)
    r.put("avgPrice",    a.avgPrice)
    r.put("minPrice",    a.minPrice)
    r.put("maxPrice",    a.maxPrice)
    r.put("totalVolume", a.totalVolume)
    r.put("tradeCount",  a.tradeCount)
    r
  }
}

class AlertMapper(schemaStr: String) extends RichMapFunction[Alert, GenericRecord] {
  @transient private var schema: Schema = _

  override def open(config: Configuration): Unit =
    schema = new Schema.Parser().parse(schemaStr)

  override def map(a: Alert): GenericRecord = {
    val r = new GenericData.Record(schema)
    r.put("symbol",        a.symbol)
    r.put("windowStart",   a.windowStart)
    r.put("prevAvgPrice",  a.prevAvgPrice)
    r.put("currAvgPrice",  a.currAvgPrice)
    r.put("changePercent", a.changePercent)
    r
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

object FlinkJob {

  private def loadSchema(resource: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    scala.io.Source.fromInputStream(stream).mkString
  }

  lazy val TradeSchemaStr: String          = loadSchema("trade.avsc")
  lazy val PriceAggregateSchemaStr: String = loadSchema("price_aggregate.avsc")
  lazy val AlertSchemaStr: String          = loadSchema("alert.avsc")

  def main(args: Array[String]): Unit = {
    val bootstrapServers  = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val schemaRegistryUrl = sys.env.getOrElse("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    val spikeThresholdPct = sys.env.get("SPIKE_THRESHOLD_PCT").map(_.toDouble).getOrElse(2.0)
    val windowSecs        = sys.env.get("WINDOW_SECONDS").map(_.toLong).getOrElse(30L)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.registerKryoType(classOf[Trade])
    env.getConfig.registerKryoType(classOf[PriceAggregate])
    env.getConfig.registerKryoType(classOf[Alert])

    val source = KafkaSource
      .builder[GenericRecord]()
      .setBootstrapServers(bootstrapServers)
      .setTopics("trades")
      .setGroupId("flink-cryptostream")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(
        ConfluentRegistryAvroDeserializationSchema.forGeneric(
          new Schema.Parser().parse(TradeSchemaStr),
          schemaRegistryUrl,
        )
      )
      .build()

    val trades = env
      .fromSource(source, WatermarkStrategy.noWatermarks[GenericRecord](), "Kafka trades")
      .map(new org.apache.flink.api.common.functions.MapFunction[GenericRecord, Trade] {
        override def map(r: GenericRecord): Trade = Trade(
          symbol    = r.get("symbol").toString,
          price     = r.get("price").asInstanceOf[Double],
          volume    = r.get("volume").asInstanceOf[Double],
          timestamp = r.get("timestamp").asInstanceOf[Long],
        )
      })

    val aggregates = trades
      .keyBy(new KeySelector[Trade, String] {
        override def getKey(t: Trade): String = t.symbol
      })
      .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSecs)))
      .aggregate(new TradeAggregator())

    val alerts = aggregates
      .keyBy(new KeySelector[PriceAggregate, String] {
        override def getKey(a: PriceAggregate): String = a.symbol
      })
      .flatMap(new SpikeDetector(spikeThresholdPct))

    aggregates
      .map(new PriceAggregateMapper(PriceAggregateSchemaStr))
      .sinkTo(avroKafkaSink(bootstrapServers, "price_aggregates", PriceAggregateSchemaStr, schemaRegistryUrl))

    alerts
      .map(new AlertMapper(AlertSchemaStr))
      .sinkTo(avroKafkaSink(bootstrapServers, "alerts", AlertSchemaStr, schemaRegistryUrl))

    env.execute("CryptoStream Flink Job")
  }

  private def avroKafkaSink(
    bootstrapServers: String,
    topic: String,
    schemaStr: String,
    registryUrl: String,
  ): KafkaSink[GenericRecord] =
    KafkaSink
      .builder[GenericRecord]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder[GenericRecord]()
          .setTopic(topic)
          .setValueSerializationSchema(
            new ConfluentAvroSerializer(schemaStr, s"$topic-value", registryUrl)
          )
          .build()
      )
      .build()
}
