//import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase, result}
//import util.ImplicitObservable._
//
//import scala.text.Document
//import scala.language.reflectiveCalls
//import java.util.concurrent.atomic.AtomicBoolean
//
//import com.mongodb.BasicDBObject
//import com.mongodb.client.model.changestream.{ChangeStreamDocument, FullDocument}
//
//import scala.collection.JavaConverters._
//import scala.concurrent.Await
//import scala.concurrent.duration.Duration
//import org.mongodb.scala.bson.conversions.Bson
//import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonNull, BsonString, BsonValue}
//
//// imports required for filters, projections and updates
//import org.bson.BsonType
//
//import org.mongodb.scala.model.Aggregates.filter
//import org.mongodb.scala.model.Filters.{and, bsonType, elemMatch, exists, gt, in, lt, lte, or}
//import org.mongodb.scala.model.Projections.{exclude, excludeId, fields, slice}
//import org.mongodb.scala.model.Updates.{combine, currentDate, set}
//// end required filters, projections and updates imports
//
//
//object CrudOperations extends App {
////  try{
////
////  }catch{
////    case exception: Exception=>println(Exception)
////  }
//
//
//
//  val mongoClient = MongoClient("mongodb://127.0.0.1:27017")
//  val database: MongoDatabase = mongoClient.getDatabase("work")
//
//  val collection = database.getCollection("record")
//  var dbObject = new BasicDBObject()
//
//  dbObject.put("name", "mongo")
//  dbObject.put("type", "db")
//
//  val students : MongoCollection[Document] = database.getCollection("students")
//
//  val doc: Document = Document(
//    "schoolStudentID" -> "9999",
//    "fullName" -> "Tom Servo",
//    "gradeLevel" -> 8
//  )
//
//
//}
//
//
