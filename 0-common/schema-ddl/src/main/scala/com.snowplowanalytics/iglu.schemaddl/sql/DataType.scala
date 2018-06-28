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
package sql

trait DataType extends Ddl

case object SqlTimestamp extends DataType {
  def toDdl = "TIMESTAMP"
}

case object SqlDate extends DataType {
  def toDdl = "DATE"
}

case object SqlSmallInt extends DataType {
  def toDdl = "SMALLINT"
}

case object SqlInteger extends DataType {
  def toDdl = "INT"
}

case object SqlBigInt extends DataType {
  def toDdl = "BIGINT"
}

case object SqlReal extends DataType {
  def toDdl = "REAL"
}

case object SqlDouble extends DataType {
  def toDdl = "DOUBLE PRECISION"
}

case class SqlDecimal(precision: Option[Int], scale: Option[Int]) extends DataType {
  def toDdl = (precision, scale) match {
    case (Some(p), Some(s)) => s"DECIMAL ($p, $s)"
    case _ => "DECIMAL"
  }
}

case object SqlBoolean extends DataType {
  def toDdl = "BOOLEAN"
}

case class SqlVarchar(size: Int) extends DataType {
  def toDdl = s"VARCHAR($size)"
}

case class SqlChar(size: Int) extends DataType {
  def toDdl = s"CHAR($size)"
}

