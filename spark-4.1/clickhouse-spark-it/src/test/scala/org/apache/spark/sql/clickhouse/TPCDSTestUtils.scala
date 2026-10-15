/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.clickhouse

object TPCDSTestUtils {
  val tablePrimaryKeys: Map[String, Seq[String]] = Map(
    "call_center" -> Array("cc_call_center_sk"),
    "catalog_page" -> Array("cp_catalog_page_sk"),
    "catalog_returns" -> Array("cr_item_sk", "cr_order_number"),
    "catalog_sales" -> Array("cs_item_sk", "cs_order_number"),
    "customer" -> Array("c_customer_sk"),
    "customer_address" -> Array("ca_address_sk"),
    "customer_demographics" -> Array("cd_demo_sk"),
    "date_dim" -> Array("d_date_sk"),
    "household_demographics" -> Array("hd_demo_sk"),
    "income_band" -> Array("ib_income_band_sk"),
    "inventory" -> Array("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"),
    "item" -> Array("i_item_sk"),
    "promotion" -> Array("p_promo_sk"),
    "reason" -> Array("r_reason_sk"),
    "ship_mode" -> Array("sm_ship_mode_sk"),
    "store" -> Array("s_store_sk"),
    "store_returns" -> Array("sr_item_sk", "sr_ticket_number"),
    "store_sales" -> Array("ss_item_sk", "ss_ticket_number"),
    "time_dim" -> Array("t_time_sk"),
    "warehouse" -> Array("w_warehouse_sk"),
    "web_page" -> Array("wp_web_page_sk"),
    "web_returns" -> Array("wr_item_sk", "wr_order_number"),
    "web_sales" -> Array("ws_item_sk", "ws_order_number"),
    "web_site" -> Array("web_site_sk")
  )
}
