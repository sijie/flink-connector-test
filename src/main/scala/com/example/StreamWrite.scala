package com.example

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.pulsar.{FlinkPulsarSink, TopicKeyExtractor}

object StreamWrite {
  def main(args: Array[String]): Unit = {

    val webServiceUrl = "http://localhost:8080"
    val brokerServiceUrl = "pulsar://localhost:6650"

    val prop = new Properties()
    prop.setProperty("service.url", brokerServiceUrl)
    prop.setProperty("admin.url", webServiceUrl)
    prop.setProperty("topic", args(0))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    implicit val ti = TypeInformation.of(classOf[NasaMission])

    val ds = env.fromCollection(
      NasaMission(1, "Mercury program", 1959, 1963) ::
      NasaMission(2, "Apollo program", 1961, 1972) ::
      NasaMission(3, "Gemini program", 1963, 1966) ::
      NasaMission(4, "Skylab", 1973, 1974) ::
      NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975) :: Nil)

    ds.addSink(new FlinkPulsarSink[NasaMission](
      brokerServiceUrl,
      webServiceUrl,
      java.util.Optional.of(args(0)),
      prop,
      new TopicKeyExtractor[NasaMission] {
        override def serializeKey(element: NasaMission): Array[Byte] = null
        override def getTopic(element: NasaMission): String = null
      }, NasaMission.getClass.asInstanceOf))

    env.execute("Flink write NASA data to pulsar.")
  }
}
