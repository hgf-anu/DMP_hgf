package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool{

	val config = new JedisPoolConfig

	config.setMaxTotal(20)

	config.setMaxIdle(10)

	val pool = new JedisPool( config, "hadoop0001", 6379, 10000, "123" )
	pool
	def getConnection():Jedis={
		pool.getResource
	}
}
