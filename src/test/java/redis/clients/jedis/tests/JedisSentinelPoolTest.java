package redis.clients.jedis.tests;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Test;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisSentinelPoolTest extends JedisTestBase {
    private static final int MASTER_PORT = 9381, SLAVE_PORT = MASTER_PORT + 1, SENTINEL1_PORT = MASTER_PORT + 20000, SENTINEL2_PORT = SENTINEL1_PORT + 1;

    protected final Set<String> sentinels = new HashSet<String>();
    private final List<EmbeddedRedis> serverProcs = new ArrayList<EmbeddedRedis>();
    private final List<EmbeddedRedis> sentinelProcs = new ArrayList<EmbeddedRedis>();

    @After
    public void tearDown() throws InterruptedException {
        for (EmbeddedRedis redis : serverProcs) {
            redis.die();
        }
        for (EmbeddedRedis redis : sentinelProcs) {
            redis.die();
        }
    }

    private void runSubscription(final String host, final int port, final JedisPubSub pubSub, final String... channels) {
        new Thread() {
            public void run() {
                Jedis sentinelJedis = new Jedis(host, port);
                sentinelJedis.subscribe(pubSub, channels);
            }
        }.start();
    }

    private void startRedisServer(String instanceName, int port, String...additionalArgs) throws IOException {
        String[] baseArgs = {"/usr/local/bin/redis-server", "--port", "" + port,
                "--save", "\"\"", "--appendonly", "no",
                "--requirepass", "foobared",
                "--logfile", "/tmp/" + instanceName + ".out"};
        String[] allArgs = new String[baseArgs.length + additionalArgs.length];
        System.arraycopy(baseArgs, 0, allArgs, 0, baseArgs.length);
        System.arraycopy(additionalArgs, 0, allArgs, baseArgs.length, additionalArgs.length);
        serverProcs.add(new EmbeddedRedis(instanceName, allArgs));
    }

    private void awaitChannels(final String host, final int port, final CountDownLatch sentinelReady, String... channelsToAwait) {
        final Set<String> awaiting = new HashSet<String>();
        for (String channel : channelsToAwait) {
            awaiting.add(channel);
        }
        JedisPubSub sentinelPubSub = new JedisPubSubAdapter(){
            @Override
            public void onMessage(String channel, String message) {
                if (awaiting.remove(channel) && awaiting.isEmpty()) {
                    sentinelReady.countDown();
                    unsubscribe();
                }
            }
        };
        runSubscription(host, port, sentinelPubSub, channelsToAwait);
    }

    private void startRedisSentinel(String instanceName, int port, String masterName, int masterPort, CountDownLatch sentinelReady, String...channelsToAwait) throws Exception {
        sentinelProcs.add(new EmbeddedRedis(instanceName, "/usr/local/bin/redis-sentinel", "--port", "" + port,
                "--logfile", "/tmp/" + instanceName + ".out",
                "--sentinel", "monitor", masterName, "127.0.0.1", "" + masterPort, "2",
                "--sentinel", "auth-pass", masterName, "foobared",
                "--sentinel", "down-after-milliseconds", masterName, "5000",
                "--sentinel", "can-failover", masterName, "yes",
                "--sentinel", "parallel-syncs", masterName, "1",
                "--sentinel", "failover-timeout", masterName, "1000"));
        sentinels.add("localhost:" + port);
        // Wait a tenth of a second for the sentinel to start up
        Thread.sleep(100);
        awaitChannels("localhost", port, sentinelReady, channelsToAwait);
    }

    private void monitorSentinelSwitch(String channel) throws InterruptedException {
        CountDownLatch sentinelSwitch = new CountDownLatch(2);
        awaitChannels("localhost", SENTINEL1_PORT, sentinelSwitch, channel);
        awaitChannels("localhost", SENTINEL2_PORT, sentinelSwitch, channel);
        sentinelSwitch.await(1, TimeUnit.MINUTES);
    }
    @Test
    public void startWithoutSentinels() throws Exception {
        startMasterSlaveRedisServers();
        sentinels.add("localhost:" + SENTINEL1_PORT);
        sentinels.add("localhost:" + SENTINEL2_PORT);

        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels,
                new Config(), 1000, "foobared", 2, 10);
        try {
            pool.getResource();
            fail("Should've thrown a JedisConnectionException");
        } catch (JedisConnectionException jce) { }

        startRedisSentinels("+sentinel");

        assertEquals("PONG", pool.getResource().ping());
    }

    @Test
    public void startWithoutServers() throws Exception {

        startRedisSentinels("+sdown");

        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels,
                new Config(), 1000, "foobared", 2, 10);

        try {
            pool.getResource();
            fail("Should've thrown a JedisConnectionException");
        } catch (JedisConnectionException jce) { }

        startMasterSlaveRedisServers();
        monitorSentinelSwitch("-sdown");

        assertEquals("PONG", pool.getResource().ping());
    }

    @Test
    public void segfaultMaster() throws Exception {
        startMasterSlaveRedisServers();

        startRedisSentinels("+sentinel", "+slave");

        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels,
                new Config(), 1000, "foobared", 2);

        Jedis jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());

        segfaultMasterJedis();

        monitorSentinelSwitch("+sentinel");

        jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());
        assertEquals("foobared", jedis.configGet("requirepass").get(1));
        assertEquals(2, jedis.getDB().intValue());
    }

    private void segfaultMasterJedis() {
        Jedis masterJedis = new Jedis("localhost", MASTER_PORT);
        masterJedis.auth("foobared");
        try {
            masterJedis.debug(DebugParams.SEGFAULT());
            fail("Should've thrown a JedisConnectionException");
        } catch (JedisConnectionException jce) {
            // We expect an exception as the connection will be closed from the segfault
        }
    }

    @Test
    public void recoverFromSentinelOutage() throws Exception {
        startMasterSlaveRedisServers();
        startRedisSentinels("+sentinel", "+slave");

        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels,
                new Config(), 1000, "foobared", 2);

        Jedis jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());

        for (EmbeddedRedis redis : sentinelProcs) {
            redis.die();
        }

        startRedisSentinels("+sentinel");

        jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());
        assertEquals("foobared", jedis.configGet("requirepass").get(1));
        assertEquals(2, jedis.getDB().intValue());
    }

    private void startRedisSentinels(String...channelsToAwait) throws Exception {
        CountDownLatch sentinelReady = new CountDownLatch(2);
        startRedisSentinel("poolsentinel1", SENTINEL1_PORT, "mymaster", MASTER_PORT, sentinelReady, channelsToAwait);
        startRedisSentinel("poolsentinel2", SENTINEL2_PORT, "mymaster", MASTER_PORT, sentinelReady, channelsToAwait);
        if (!sentinelReady.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Sentinels weren't ready in 15 seconds");
        }
    }

    private void startMasterSlaveRedisServers() throws IOException {
        startRedisServer("master", MASTER_PORT);
        startRedisServer("slave", SLAVE_PORT, "--slaveof", "localhost", "" + MASTER_PORT, "--masterauth", "foobared");
    }
}
