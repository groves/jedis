package redis.clients.util;

public interface Pool<T> {
    T getResource();
    int getNumActive();
    void returnBrokenResource(T resource);
    void returnResource(T resource);
    void destroy();
}