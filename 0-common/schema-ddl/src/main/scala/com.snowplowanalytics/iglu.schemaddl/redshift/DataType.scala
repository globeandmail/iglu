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

import sql.DataType

/**
 * Data types
 * http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
 */

// CUSTOM
/**
 * These predefined data types assembles into usual Redshift data types, but
 * can store additional information such as warnings.
 * Using to prevent output on DDL-generation step.
 */
case class ProductType(override val warnings: List[String]) extends DataType {
  def toDdl = "VARCHAR(4096)"
}

