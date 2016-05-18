package com.creek.streaming.framework.producer.effect.sceneBranch.object;

import java.util.HashMap;

public class MemKey {    
    private HashMap<String, String> map = null;
    
    public MemKey(HashMap<String, String> map) {
        super();
        this.map = map;
    }
    
    public void setValue(String key, String val){
        if(map == null)
            map = new HashMap<String, String>();
        map.put(key, val);
    }
    
    public String getValue(String key){
        if(map != null && map.containsKey(key))
            return map.get(key);
        else
            return null;
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
        MemKey other = (MemKey) obj;
        if (map == null) {
            if (other.map != null)
                return false;
        } else if (!map.equals(other.map))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MemKey [map=" + map + "]";
    }        
}
