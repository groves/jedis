package redis.clients.jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class JedisSentinelPool implements Pool<Jedis> {
    private GenericObjectPool<Jedis> internalPool;

    protected Config poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    protected Logger log = Logger.getLogger(getClass().getName());

    private volatile HostAndPort currentHostMaster;

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new Config(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig, int timeout, String password) {
        this(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig, int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig, String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig, int timeout, String password, int database) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;

        HostAndPort master = initSentinels(sentinels, masterName);
        initPool(master);
    }

    public Jedis getResource() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }


    public void returnBrokenResource(Jedis resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void returnResource(Jedis resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }

    private class HostAndPort {
        String host;
        int port;

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof HostAndPort) {
                HostAndPort hp = (HostAndPort) obj;
                return port == hp.port && host.equals(hp.host);
            }
            return false;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private void initPool(HostAndPort master) {
        if (!master.equals(currentHostMaster)) {
            currentHostMaster = master;
            log.info("Created JedisPool to master at " + master);
            internalPool =
                    new GenericObjectPool<Jedis>(new JedisFactory(master.host, master.port, timeout, password, database), poolConfig);
        }
    }

    private HostAndPort initSentinels(Set<String> sentinels, final String masterName) {

        HostAndPort master = null;
        boolean running = true;

        outer:
        while (running) {

            log.info("Trying to find master from available Sentinels...");

            for (String sentinel : sentinels) {

                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

                log.fine("Connecting to Sentinel " + hap);

                try {
                    Jedis jedis = new Jedis(hap.host, hap.port);

                    if (master == null) {
                        master = toHostAndPort(jedis.sentinelGetMasterAddrByName(masterName));
                        log.fine("Found Redis master at " + master);
                        jedis.disconnect();
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log.warning("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                }
            }

            try {
                log.severe("All sentinels down, cannot determine where is "
                        + masterName + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("Redis master running at " + master + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
            MasterListener masterListener = new MasterListener(masterName, hap);
            masterListeners.add(masterListener);
            masterListener.start();
        }

        return master;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        final HostAndPort hap = new HostAndPort();
        hap.host = getMasterAddrByNameResult.get(0);
        hap.port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return hap;
    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected HostAndPort hostAndPort;
        protected long subscribeRetryWaitTimeMillis = 5000;
        protected Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        public MasterListener(String masterName, HostAndPort hostAndPort) {
            this.masterName = masterName;
            this.hostAndPort = hostAndPort;
        }

        public void run() {

            running.set(true);

            while (running.get()) {

                j = new Jedis(hostAndPort.host, hostAndPort.port);

                try {
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            log.fine("Sentinel " + hostAndPort + " published: " + message + ".");

                            String[] switchMasterMsg = message.split(" ");

                            if (switchMasterMsg.length > 3) {

                                if (masterName.equals(switchMasterMsg[0])) {
                                    initPool(toHostAndPort(Arrays.asList(
                                            switchMasterMsg[3],
                                            switchMasterMsg[4])));
                                } else {
                                    log.fine("Ignoring message on +switch-master for master name " + switchMasterMsg[0]
                                            + ", our master name is " + masterName);
                                }

                            } else {
                                log.severe("Invalid message received on Sentinel " + hostAndPort +
                                        " on channel +switch-master: " + message);
                            }
                        }
                    }, "+switch-master");

                } catch (JedisConnectionException e) {

                    if (running.get()) {
                        log.severe("Lost connection to Sentinel at " + hostAndPort + ". Sleeping 5000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        log.fine("Unsubscribing from Sentinel at " + hostAndPort);
                    }
                }
            }
        }

        public void shutdown() {
            try {
                log.fine("Shutting down listener on " + hostAndPort);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                j.disconnect();
            } catch (Exception e) {
                log.severe("Caught exception while shutting down: " + e.getMessage());
            }
        }
    }
}