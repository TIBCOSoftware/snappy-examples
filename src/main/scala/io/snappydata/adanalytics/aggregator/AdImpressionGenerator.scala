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

import io.snappydata.adanalytics.aggregator.Constants._

import java.util.Random

object AdImpressionGenerator {
  def generateAdImpression(): AdImpressionLog = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val publisher = publishers(random.nextInt(numPublishers-10+1)+10)
    val advertiser = advertisers(random.nextInt(numAdvertisers-10+1)+10)
    val website = websites(random.nextInt(numWebsites-100+1)+100)
    val cookie = cookies(random.nextInt(numCookies-100+1)+100)
    val geo = geos(random.nextInt(geos.size))
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
