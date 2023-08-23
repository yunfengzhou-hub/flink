package org.apache.flink.runtime.state.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class LinkedHashMapLRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;

    private final BiConsumer<K, V> removalCallback;

    public LinkedHashMapLRUCache(int capacity, BiConsumer<K, V> removalCallback) {
        // Make sure the map need not be rehashed.
        super(capacity + capacity / 50 + 2, 0.99F, true);
        this.capacity = capacity;
        this.removalCallback = removalCallback;
    }

    @Override
    public V get(Object key) {
        return super.get(key);
    }

    @Override
    public V put(K key, V value) {
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (size() > capacity) {
            removalCallback.accept(eldest.getKey(), eldest.getValue());
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LinkedHashMapLRUCache<K, V> clone() {
        return (LinkedHashMapLRUCache<K, V>) super.clone();
    }
}
