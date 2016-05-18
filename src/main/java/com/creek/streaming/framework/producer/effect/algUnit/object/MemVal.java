package com.creek.streaming.framework.producer.effect.algUnit.object;

import java.util.HashMap;

public class MemVal {
    private HashMap<String, Long> map = null;
    
    public MemVal(HashMap<String, Long> map) {
        super();
        this.map = map;
    }
    
    public void setValue(String key, long val){
        if(map == null)
            map = new HashMap<String, Long>();
        map.put(key, val);
    }
    
    public long getValue(String key){
        if(map != null && map.containsKey(key))
            return map.get(key);
        else
            return 0L;
    }

    @Override
    public String toString() {
        return "MemVal [map=" + map + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((map == null) ? 0 : map.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MemVal other = (MemVal) obj;
        if (map == null) {
            if (other.map != null)
                return false;
        } else if (!map.equals(other.map))
            return false;
        return true;
    }
}
