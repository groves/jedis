package redis.clients.util;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> {
    protected GenericObjectPool<T> internalPool;

    /**
     * Using this constructor means you have to set
     * and initialize the internalPool yourself.
     */
    public Pool() {}
    
    public Pool(final GenericObjectPool.Config poolConfig, PoolableObjectFactory<T> factory) {
        initPool(poolConfig, factory);
    }
    
    public void initPool(final GenericObjectPool.Config poolConfig, PoolableObjectFactory<T> factory) {
    	
    	if (this.internalPool != null) {
    		try {
    			destroy();
    		} catch (Exception e) {    			
    		}
    	}
    	
    	this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
    }
    
    public T getResource() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException(
                    "Could not get a resource from the pool", e);
        }
    }
        

    public void returnBrokenResource(final T resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }
    
    public void returnResource(final T resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }
}