package io.cryptostream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
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
  var symbol: String         = ""
  var priceSum: Double       = 0.0
  var minPrice: Double       = Double.MaxValue
  var maxPrice: Double       = Double.MinValue
  var totalVolume: Double    = 0.0
  var tradeCount: Long       = 0L
  var firstTimestamp: Long   = Long.MaxValue
  var lastTimestamp: Long    = Long.MinValue
}

// ---------------------------------------------------------------------------
// Windowed aggregation — outputs PriceAggregate directly, no ProcessWindowFunction
// needed (avoids Scala 2.12 inner-class / Iterable override incompatibilities).
// Window bounds are derived from event timestamps in the accumulator.
// ---------------------------------------------------------------------------

class TradeAggregator extends AggregateFunction[Trade, TradeAccum, PriceAggregate] {
  override def createAccumulator(): TradeAccum = new TradeAccum

  override def add(t: Trade, acc: TradeAccum): TradeAccum = {
    acc.symbol          = t.symbol
    acc.priceSum       += t.price
    acc.minPrice        = math.min(acc.minPrice, t.price)
    acc.maxPrice        = math.max(acc.maxPrice, t.price)
    acc.totalVolume    += t.volume
    acc.tradeCount     += 1
    acc.firstTimestamp  = math.min(acc.firstTimestamp, t.timestamp)
    acc.lastTimestamp   = math.max(acc.lastTimestamp, t.timestamp)
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
    a.priceSum        += b.priceSum
    a.minPrice         = math.min(a.minPrice, b.minPrice)
    a.maxPrice         = math.max(a.maxPrice, b.maxPrice)
    a.totalVolume     += b.totalVolume
    a.tradeCount      += b.tradeCount
    a.firstTimestamp   = math.min(a.firstTimestamp, b.firstTimestamp)
    a.lastTimestamp    = math.max(a.lastTimestamp, b.lastTimestamp)
    a
  }
}

// ---------------------------------------------------------------------------
// Spike detection — flags windows where avg price moved > threshold %
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
// Entry point
// ---------------------------------------------------------------------------

object FlinkJob {

  val TradeSchemaStr: String =
    """|{
       |  "type": "record",
       |  "name": "Trade",
       |  "namespace": "io.cryptostream",
       |  "fields": [
       |    {"name": "symbol",    "type": "string"},
       |    {"name": "price",     "type": "double"},
       |    {"name": "volume",    "type": "double"},
       |    {"name": "timestamp", "type": "long", "doc": "Unix milliseconds"}
       |  ]
       |}""".stripMargin

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
      .map(new MapFunction[GenericRecord, Trade] {
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
      .map(new MapFunction[PriceAggregate, String] {
        override def map(a: PriceAggregate): String =
          s"""{"symbol":"${a.symbol}","windowStart":${a.windowStart},"windowEnd":${a.windowEnd},"avgPrice":${a.avgPrice},"minPrice":${a.minPrice},"maxPrice":${a.maxPrice},"totalVolume":${a.totalVolume},"tradeCount":${a.tradeCount}}"""
      })
      .sinkTo(kafkaSink(bootstrapServers, "price_aggregates"))

    alerts
      .map(new MapFunction[Alert, String] {
        override def map(a: Alert): String =
          s"""{"symbol":"${a.symbol}","windowStart":${a.windowStart},"prevAvgPrice":${a.prevAvgPrice},"currAvgPrice":${a.currAvgPrice},"changePercent":${a.changePercent}}"""
      })
      .sinkTo(kafkaSink(bootstrapServers, "alerts"))

    env.execute("CryptoStream Flink Job")
  }

  private def kafkaSink(bootstrapServers: String, topic: String): KafkaSink[String] =
    KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder[String]()
          .setTopic(topic)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build()
}
