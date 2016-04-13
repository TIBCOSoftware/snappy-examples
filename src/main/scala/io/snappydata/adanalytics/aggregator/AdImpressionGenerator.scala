/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.adanalytics.aggregator

import Constants._

import scala.util.Random

class AdImpressionGenerator {
  def generateAdImpression() = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val publisher = Publishers(random.nextInt(NumPublishers))
    val advertiser = Advertisers(random.nextInt(NumAdvertisers))
    val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
    val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
    val geo = Geos(random.nextInt(Geos.size))
    val bid = math.abs(random.nextDouble()) % 1

    val log = new AdImpressionLog()
    log.setTimestamp(timestamp)
    log.setPublisher(publisher)
    log.setAdvertiser(advertiser)
    log.setWebsite(website)
    log.setGeo(geo)
    log.setBid(bid)
    log.setCookie(cookie)
    log
  }
}
