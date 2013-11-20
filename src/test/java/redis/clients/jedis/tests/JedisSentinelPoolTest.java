package redis.clients.jedis.tests;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisSentinelPoolTest extends JedisTestBase {
    private static final int MASTER_PORT = 9381, SLAVE_PORT = MASTER_PORT + 1, SENTINEL1_PORT = MASTER_PORT + 20000, SENTINEL2_PORT = SENTINEL1_PORT + 1;

    protected final Set<String> sentinels = new HashSet<String>();
    private final List<EmbeddedRedis> redii = new ArrayList<EmbeddedRedis>();

    @After
    public void tearDown() {
        for (EmbeddedRedis redis : redii) {
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
        redii.add(new EmbeddedRedis(instanceName, allArgs));
    }

    private void monitorSentinelReadiness(final String host, final int port, final CountDownLatch sentinelReady) {
        JedisPubSub sentinelPubSub = new JedisPubSubAdapter(){
            @Override
            public void onMessage(String channel, String message) {
                Jedis reconn = new Jedis(host, port);
                List<Map<String, String>> masters = reconn.sentinelMasters();
                List<Map<String, String>> slaves = reconn.sentinelSlaves("mymaster");
                if (!masters.isEmpty() && !slaves.isEmpty()) {
                    sentinelReady.countDown();
                    unsubscribe();
                }
            }
        };
        runSubscription(host, port, sentinelPubSub, "+sentinel", "+slave");
    }

    private void startRedisSentinel(String instanceName, int port, String masterName, int masterPort, CountDownLatch sentinelReady) throws IOException {
        redii.add(new EmbeddedRedis(instanceName, "/usr/local/bin/redis-sentinel", "--port", "" + port,
                "--logfile", "/tmp/" + instanceName + ".out",
                "--sentinel", "monitor", masterName, "127.0.0.1", "" + masterPort, "2",
                "--sentinel", "auth-pass", masterName, "foobared",
                "--sentinel", "down-after-milliseconds", masterName, "5000",
                "--sentinel", "can-failover", masterName, "yes",
                "--sentinel", "parallel-syncs", masterName, "1",
                "--sentinel", "failover-timeout", masterName, "1000"));
        sentinels.add("localhost:" + port);
        monitorSentinelReadiness("localhost", port, sentinelReady);
    }

    @Before
    public void setUp() throws Exception {
        startRedisServer("master", MASTER_PORT);
        startRedisServer("slave", SLAVE_PORT, "--slaveof", "localhost", "" + MASTER_PORT, "--masterauth", "foobared");

        CountDownLatch sentinelReady = new CountDownLatch(2);
        startRedisSentinel("poolsentinel1", SENTINEL1_PORT, "mymaster", MASTER_PORT, sentinelReady);
        startRedisSentinel("poolsentinel2", SENTINEL2_PORT, "mymaster", MASTER_PORT, sentinelReady);
        if (!sentinelReady.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Sentinels weren't ready in 15 seconds");
        }
    }

    private void monitorSentinelSwitch(String host, int port, final CountDownLatch onSwitch) {
        runSubscription(host, port, new JedisPubSubAdapter() {
            @Override
            public void onMessage(String channel, String message) {
                onSwitch.countDown();
                unsubscribe();
            }
        }, "+sentinel");
    }

    @Test
    public void segfaultMaster() throws InterruptedException {
        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels,
                new Config(), 1000, "foobared", 2);

        Jedis jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());

        Jedis masterJedis = new Jedis("localhost", MASTER_PORT);
        masterJedis.auth("foobared");
        try {
            masterJedis.debug(DebugParams.SEGFAULT());
        } catch (JedisConnectionException jce) {
            // We expect an exception as the connection will be closed from the segfault
        }

        CountDownLatch sentinelSwitch = new CountDownLatch(2);
        monitorSentinelSwitch("localhost", SENTINEL1_PORT, sentinelSwitch);
        monitorSentinelSwitch("localhost", SENTINEL2_PORT, sentinelSwitch);
        sentinelSwitch.await(1, TimeUnit.MINUTES);

        jedis = pool.getResource();
        assertEquals("PONG", jedis.ping());
        assertEquals("foobared", jedis.configGet("requirepass").get(1));
        assertEquals(2, jedis.getDB().intValue());
    }
}
