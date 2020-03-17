/*
* Copyright © 2019. TIBCO Software Inc.
* This file is subject to the license terms contained
* in the license file that is distributed with this file.
*/
package io.snappydata.adanalytics

import java.util.Random

import Configs._

object AdImpressionGenerator {

  def nextRandomAdImpression(): AdImpressionLog = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val publisher = publishers(random.nextInt(numPublishers - 10 + 1) + 10)
    val advertiser = advertisers(random.nextInt(numAdvertisers - 10 + 1) + 10)
    val website = websites(random.nextInt(numWebsites - 100 + 1) + 100)
    val cookie = cookies(random.nextInt(numCookies - 100 + 1) + 100)
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

  def nextNormalAdImpression(): AdImpressionLog = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val publisher = publishers(getGaussian(random, numPublishers / 2, 8, numPublishers))
    val advertiser = advertisers(getGaussian(random, numAdvertisers / 2, 5, numAdvertisers))
    val website = websites(getGaussian(random, numWebsites / 2, 160, numWebsites))
    val cookie = cookies(getGaussian(random, numCookies / 2, 160, numCookies))
    val geo = geos(getGaussian(random, numGeos / 2, 8, numGeos - 1))
    val bid = math.abs(random.nextGaussian()) % 1

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

  def getGaussian(rand: Random, mean: Int, std: Int, range: Int): Int = {
    var gauss = 0
    do {
      val g = rand.nextGaussian()
      gauss = Math.round(mean + std * g).toInt
    } while (gauss < 0 || gauss > range)
    gauss
  }
}
