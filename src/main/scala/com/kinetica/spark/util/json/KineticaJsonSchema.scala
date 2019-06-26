package com.kinetica.spark.util.json

import com.gpudb.protocol.ShowTypesResponse
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.functional.syntax.{unlift, _}
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map

object KineticaJsonSchema extends LazyLogging {

  // root class used to make json file compatible with inclusion in other metadata readers
  case class KineticaSchemaRoot(kinetica_schema: Option[KineticaSchemaMetadata] = None)

  case class KineticaSchemaMetadata(typeProperties: Map[String, Seq[String]], typeSchema: Option[KineticaSchemaDefinition], isReplicated: Boolean = false)

  case class KineticaSchemaDefinition(recordType: String, name: String, fields: Seq[KineticaFieldDefinition])

  case class KineticaFieldDefinition(name: String, typeProperties: Seq[String])

  object implicits {

    implicit val formatKineticaFieldDefinition = new Format[KineticaFieldDefinition] {
      override def writes(field: KineticaFieldDefinition): JsValue = Json.obj(
        "name" -> JsString(field.name),
        "type" -> {
          if (field.typeProperties.length == 1) {
            // represent as string in json
            JsString(field.typeProperties.head)
          } else {
            // represent as array in json
            JsArray(field.typeProperties.map(s => JsString(s)))
          }
        }
      )

      override def reads(json: JsValue): JsResult[KineticaFieldDefinition] = {
        try {
          val name = json \ "name" match {
            case JsDefined(JsString(str)) => str
            case _ => throw new Exception("cannot parse name")
          }
          val types = json \ "type" match {
            case JsDefined(JsArray(value)) => value.map(_.as[String])
            case JsDefined(JsString(str)) => Seq(str)
            case _ => throw new Exception("cannot parse type")
          }
          JsSuccess(KineticaFieldDefinition(name, types))
        }
        catch {
          case _: Throwable => {
            logger.error(s"unable to parse json: $json")
            JsError(s"unable to parse json: $json")
          }
        }
      }
    }

    implicit val writesKineticaSchemaDefinition = (
      (JsPath \ "type").write[String] and
        (JsPath \ "name").write[String] and
        (JsPath \ "fields").write[Seq[KineticaFieldDefinition]]
      ) (unlift(KineticaSchemaDefinition.unapply))

    implicit val readsKineticaSchemaDefinition = (
      (JsPath \ "type").read[String] and
        (JsPath \ "name").read[String] and
        (JsPath \ "fields").read[Seq[KineticaFieldDefinition]]
      ) (KineticaSchemaDefinition.apply _)


    implicit val schemaMapReads: Reads[Map[String, Seq[String]]] = new Reads[Map[String, Seq[String]]] {
      def reads(jv: JsValue): JsResult[Map[String, Seq[String]]] = {
        JsSuccess(jv.as[scala.collection.immutable.Map[String, Array[String]]].map {
          case (k, v) => {
            //println(s"$k -> ${v.toSeq}")
            k -> v.toSeq
          }
        })
      }
    }

    implicit val kineticaModelFormat = Json.using[Json.WithDefaultValues].format[KineticaSchemaMetadata]

    implicit val kineticaSchemaRootModel = Json.using[Json.WithDefaultValues].format[KineticaSchemaRoot]
  }

  def decodeSchemaFromJson(jsonString: String): KineticaSchemaRoot = {
    import implicits._

    val result = Json.fromJson[KineticaSchemaRoot](Json.parse(jsonString))
    if(result.isSuccess)
    {
      result.get
    } else {
      logger.error(result.toString)
      throw new Exception(result.toString)
    }
  }

  def encodeSchemaToJson(kineticaSchemaRoot: KineticaSchemaRoot) = {
    import implicits._
    Json.prettyPrint(Json.toJson[KineticaSchemaRoot](kineticaSchemaRoot))
  }

  def decodeKineticaSchemaDefinition(jsonString: String): Option[KineticaSchemaDefinition] = {
    import implicits._
    Some(Json.fromJson[KineticaSchemaDefinition](Json.parse(jsonString)).get)
  }

  def encodeKineticaSchemaDefinition(schema: Option[KineticaSchemaDefinition]): String  = {
    import implicits._
    Json.prettyPrint(Json.toJson[KineticaSchemaDefinition](schema.get))
  }

  def toKineticaSchemaMetadata(typeInfo: ShowTypesResponse, isReplicated: Boolean = false): KineticaSchemaMetadata = {
    val props = typeInfo.getProperties.asScala(0).asScala.mapValues(_.toSeq)
    val typeDef = decodeKineticaSchemaDefinition(typeInfo.getTypeSchemas.asScala(0))
    KineticaSchemaMetadata(props.toMap, typeDef, isReplicated)
  }


}
