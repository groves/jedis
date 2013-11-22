package redis.clients.jedis;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class JedisSentinelPool implements Pool<Jedis> {
    private static class Address {
        public final String host;
        public final int port;

        private Address(String host, String port) {
            this.host = host;
            this.port = Integer.parseInt(port);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Address) {
                Address hp = (Address) obj;
                return port == hp.port && host.equals(hp.host);
            }
            return false;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private static class ActivePool {
        public final GenericObjectPool<Jedis> pool;
        public final ConcurrentMap<Jedis, Object> borrowedObjects = new ConcurrentHashMap<Jedis, Object>();
        public final Address address;

        private ActivePool(GenericObjectPool<Jedis> pool, Address address) {
            this.pool = pool;
            this.address = address;
        }

        public Jedis borrowObject() throws Exception {
            Jedis borrowed = pool.borrowObject();
            borrowedObjects.put(borrowed, new Object());
            return borrowed;  //To change body of created methods use File | Settings | File Templates.
        }

        public void returnBrokenResource(Jedis resource) {
            if (borrowedObjects.remove(resource) == null) {
                return;
            }
            try {
                pool.invalidateObject(resource);
            } catch (Exception e) {
                throw new JedisException("Could not return the resource to the pool", e);
            }
        }

        public void returnResource(Jedis resource) {
            if (borrowedObjects.remove(resource) == null) {
                return;
            }
            try {
                pool.returnObject(resource);
            } catch (Exception e) {
                throw new JedisException("Could not return the resource to the pool", e);
            }
        }
    }

    /** The currently active pool and its connections or null if we have no active pool. */
    private ActivePool pool;

    protected final Config poolConfig;

    protected final int timeout;

    protected final String password;

    protected final int database;

    protected final long subscribeRetryWaitTimeMillis;

    protected final Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    protected Logger log = Logger.getLogger(getClass().getName());

    private final CountDownLatch hasPerformedInitialSentinelCheck;

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
        this(masterName, sentinels, poolConfig, timeout, password, database, 5000);
    }

    public JedisSentinelPool(String masterName, Set<String> sentinels, Config poolConfig, int timeout, String password, int database, long subscribeRetryWaitTimeMillis) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        hasPerformedInitialSentinelCheck = new CountDownLatch(sentinels.size());

        if (sentinels.isEmpty()) {
            throw new IllegalArgumentException("Must include at least one sentinel");
        }

        // Parse all the addresses first in case they're malformed
        List<Address> addresses = new ArrayList<Address>();
        for (String sentinel : sentinels) {
            try {
                String[] hostAndPort = sentinel.split(":");
                Address hap = new Address(hostAndPort[0], hostAndPort[1]);
                addresses.add(hap);
            } catch (RuntimeException re) {
                throw new IllegalArgumentException("Sentinel address" + sentinel + " is malformed", re);
            }
        }
        for (Address address : addresses) {
            masterListeners.add(new MasterListener(masterName, address));
        }
    }

    public Jedis getResource() {
        // If we don't have a pool, check if any of the initial sentinel checks are still active. If so, wait a second
        // before assuming we don't have a master available. This should only happen for the first few seconds after the
        // creation of this class i.e. until all the MasterListner threads get past their first get master calls.
        if (pool == null) {
            try {
                hasPerformedInitialSentinelCheck.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new JedisConnectionException("Interrupted while waiting to see if initial sentinel check will pass", e);
            }
        }

        // Grab a reference to the pool first in case it's assigned as we're going through the method
        ActivePool currentPool = pool;
        if (currentPool == null) {
            // No pool means we either haven't gotten a master from the sentinels yet, or there was one and it failed
            throw new JedisConnectionException("No master available from the sentinels");
        }
        try {
            return currentPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }

    public void returnBrokenResource(Jedis resource) {
        ActivePool currentPool = pool;
        if (currentPool == null) {
            return;
        }
        currentPool.returnBrokenResource(resource);
    }

    public void returnResource(Jedis resource) {
        ActivePool currentPool = pool;
        if (currentPool == null) {
            return;
        }
        currentPool.returnResource(resource);
    }

    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }
        ActivePool currentPool = pool;
        if (currentPool == null) {
            return;
        }
        try {
            currentPool.pool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }

    private void setMaster(Address master) {
        synchronized (poolConfig) {
            ActivePool currentPool = pool;
            if (currentPool != null && master.equals(currentPool.address)) {
                return;
            }
            try {
                currentPool.pool.close();
            } catch (Exception e) {
                log.fine("Failed closing the previous pool on a master switch: " + e);
            }
            log.info("Created JedisPool to master at " + master);
            pool = new ActivePool(new GenericObjectPool<Jedis>(new JedisFactory(master.host, master.port, timeout, password, database), poolConfig), master);
        }
    }

    protected class MasterListener extends Thread {

        private final String masterName;
        private final Address sentinelAddress;
        private Jedis j;
        private volatile boolean stop;
        private String lastAttemptFailureReason;

        public MasterListener(String masterName, Address sentinelAddress) {
            this.masterName = masterName;
            this.sentinelAddress = sentinelAddress;
            start();
        }

        public void run() {
            // While we're not stopped, create a new client to the sentinel, fetch the current master, and then
            // subscribe for updates over and over again. If we're kicked out of either method, it's because we failed
            // in talking to the sentinel, so we should start over from fetching the master in case things changed while
            // we weren't subscribed.
            while (!stop) {
                j = new Jedis(sentinelAddress.host, sentinelAddress.port);

                fetchMaster();
                if (lastAttemptFailureReason != null) {
                    continue;
                }
                hasPerformedInitialSentinelCheck.countDown();
                subscribeToMasterSwitches();
            }
        }

        private void subscribeToMasterSwitches() {
            try {
                j.subscribe(new JedisPubSubAdapter() {
                    @Override
                    public void onMessage(String channel, String message) {
                        log.fine("Sentinel " + sentinelAddress + " published: " + message + ".");

                        String[] switchMasterMsg = message.split(" ");

                        if (switchMasterMsg.length > 3) {

                            if (masterName.equals(switchMasterMsg[0])) {
                                setMaster(new Address(switchMasterMsg[3], switchMasterMsg[4]));
                            } else {
                                log.fine("Ignoring message on +switch-master for master name " + switchMasterMsg[0]
                                        + ", our master name is " + masterName);
                            }

                        } else {
                            log.severe("Invalid message received on Sentinel " + sentinelAddress +
                                    " on channel +switch-master: " + message);
                        }
                    }
                }, "+switch-master");

            } catch (JedisConnectionException e) {
                waitToRetry("Lost connection to Sentinel at " + sentinelAddress + ": " + e + ".");
            }
        }

        private void fetchMaster() {
            List<String> masterAddress;
            try {
                masterAddress = j.sentinelGetMasterAddrByName(masterName);
            } catch (JedisDataException jde) {
                if (jde.getMessage().startsWith("IDONTKNOW")) {
                    waitToRetry("Sentinel " + sentinelAddress + " doesn't know of a functional master.");
                } else {
                    log.severe("Unexpected JedisDataException in fetching master: " + jde);
                    throw jde;
                }
                return;
            } catch (JedisConnectionException jce) {
                waitToRetry("Lost connection to Sentinel at " + sentinelAddress + ": " + jce + ".");
                return;
            }
            Address master = new Address(masterAddress.get(0), masterAddress.get(1));
            setMaster(master);
            if (lastAttemptFailureReason != null) {
                lastAttemptFailureReason = null;
                log.warning("Sentinel " + sentinelAddress + " recovered to fetch a master at " + master);
            }
        }

        private void waitToRetry(String reason) {
            if (stop) {
                return;
            }
            try {
                // Only log when the failure reason changes.
                if (!reason.equals(lastAttemptFailureReason)) {
                    log.warning(reason + " Retrying every " + subscribeRetryWaitTimeMillis + " milliseconds.");
                }
                lastAttemptFailureReason = reason;
                Thread.sleep(subscribeRetryWaitTimeMillis);
            } catch (InterruptedException e1) {
                Thread.interrupted();
            }
        }

        public void shutdown() {
            try {
                log.fine("Shutting down listener on " + sentinelAddress);
                stop = true;
                interrupt();
            } catch (Exception e) {
                log.severe("Caught exception while shutting down: " + e.getMessage());
            }
        }
    }
}