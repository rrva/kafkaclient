class LRUCache<K, V>(private val c: Int) {
    var map: LinkedHashMap<K, V> = object : LinkedHashMap<K, V>(16, .75f, true) {
        override fun removeEldestEntry(eldest: Map.Entry<K, V>?): Boolean {
            return size > c
        }
    }

    fun get(key: K): V? {
        return map[key]
    }

    fun put(key: K, value: V) {
        map[key] = value
    }
}