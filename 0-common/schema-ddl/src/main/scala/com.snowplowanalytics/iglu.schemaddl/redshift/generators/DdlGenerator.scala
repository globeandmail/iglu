/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.iglu.schemaddl
package redshift
package generators

// Scala
import scala.annotation.tailrec

// Scalaz
import scalaz._

// This project
import sql._
import EncodeSuggestions._
import sql.generators.SqlDdlGenerator
import redshift.generators.EncodeSuggestions.EncodingSuggestion
import sql.generators.SqlTypeSuggestions.DataTypeSuggestion

/**
 * Generates a Redshift DDL File from a Flattened JsonSchema
 */
object DdlGenerator extends SqlDdlGenerator{

  /**
    * Generate DDL for atomic (with Snowplow-specific columns and attributes) table
    *
    * @param dbSchema optional redshift schema name
    * @param name table name
    * @param columns list of generated DDLs for columns
    * @return full CREATE TABLE statement ready to be rendered
    */
  protected def getAtomicTableDdl(dbSchema: Option[String], name: String, columns: List[Column]): CreateTable = {
    val schema           = dbSchema.getOrElse("atomic")
    val fullTableName    = schema + "." + name
    val tableConstraints = Set[TableConstraint](DdlDefaultForeignKey(schema))
    val tableAttributes  = Set[TableAttribute]( // Snowplow-specific attributes
      Diststyle(Key),
      DistKeyTable("root_id"),
      SortKeyTable(None, NonEmptyList("root_tstamp"))
    )

    CreateTable(
      fullTableName,
      selfDescSchemaColumns ++ parentageColumns ++ columns,
      tableConstraints,
      tableAttributes
    )
  }

  /**
    * Takes each suggestion out of ``compressionEncodingSuggestions`` and
    * decide whether current properties satisfy it, then return the compression
    * encoding.
    * If nothing suggested ZSTD Encoding returned as default
    *
    * @param properties is a string we need to recognize
    * @param dataType redshift data type for current column
    * @param columnName to produce warning
    * @param suggestions list of functions can recognize encode type
    * @return some format or none if nothing suites
    */
  @tailrec protected[schemaddl] def getEncoding(
     properties: Map[String, String],
     dataType: DataType,
     columnName: String,
     suggestions: List[EncodingSuggestion] = encodingSuggestions)
  : CompressionEncoding = {

    suggestions match {
      case Nil => CompressionEncoding(ZstdEncoding) // ZSTD is default for user-generated
      case suggestion :: tail => suggestion(properties, dataType, columnName) match {
        case Some(encoding) => CompressionEncoding(encoding)
        case None => getEncoding(properties, dataType, columnName, tail)
      }
    }
  }

  /**
    * Takes each suggestion out of ``dataTypeSuggesions`` and decide whether
    * current properties satisfy it, then return the data type
    * If nothing suggested VARCHAR with ``varcharSize`` returned as default
    *
    * @param properties is a string we need to recognize
    * @param varcharSize default size for unhandled properties and strings
    *                    without any data about length
    * @param columnName to produce warning
    * @param suggestions list of functions can recognize encode type
    * @return some format or none if nothing suites
    */
   override final protected[schemaddl] def getDataType(
     properties: Map[String, String],
     varcharSize: Int,
     columnName: String,
     suggestions: List[DataTypeSuggestion] = SqlDdlGenerator.dataTypeSuggestions :+ TypeSuggestions.productSuggestion)
  : DataType = {
    super.getDataType(properties, varcharSize, columnName, suggestions)
  }

  /**
    * Processes the Map of Data elements pulled from the JsonSchema and
    * generates DDL object for it with it's name, constrains, attributes
    * data type, etc
    *
    * @param flatDataElems The Map of Schema keys -> attributes which need to
    *                      be processed
    * @param required required fields to decide which columns are nullable
    * @return a list of Column DDLs
    */
  private[schemaddl] def getColumnsDdl(
    flatDataElems: PropertyList,
    required: Set[String],
    varcharSize: Int)
  : Iterable[Column] = {

    // Process each key pair in the map
    for {
      (columnName, properties) <- flatDataElems
    } yield {
      val dataType = getDataType(properties, varcharSize, columnName)
      val encoding = getEncoding(properties, dataType, columnName)
      val constraints =    // only "NOT NULL" now
        if (checkNullability(properties, required.contains(columnName))) Set.empty[ColumnConstraint]
        else Set[ColumnConstraint](Nullability(NotNull))
      Column(columnName, dataType, Set(encoding),  constraints)
    }
  }


  // Columns with data taken from self-describing schema
  protected[schemaddl] val selfDescSchemaColumns = List(
    Column("schema_vendor", SqlVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_name", SqlVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_format", SqlVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_version", SqlVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull)))
  )

  // Snowplow-specific columns
  protected[schemaddl] val parentageColumns = List(
    Column("root_id", SqlChar(36), Set(CompressionEncoding(RawEncoding)), Set(Nullability(NotNull))),
    Column("root_tstamp", SqlTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_root", SqlVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_tree", SqlVarchar(1500), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_parent", SqlVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull)))
  )
  // List of compression encoding suggestions
  val encodingSuggestions: List[EncodingSuggestion] = List(lzoSuggestion, zstdSuggestion)

}
