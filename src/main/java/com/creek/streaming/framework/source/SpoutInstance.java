package com.creek.streaming.framework.source;

public class SpoutInstance {
    private Object spout;
    private String spoutName;
    private int    spoutNum;//一种spout需要创建多少个
    private String groupingType;//fieldsGrouping or shuffleGrouping or ...
    private String groupingField;//当type为fieldsGrouping时，需要针对msg中哪个field做grouping 
    
    public SpoutInstance(Object spout, String spoutName, int spoutNum,String groupingType, String groupingField) {
        super();
        this.spout = spout;
        this.spoutName = spoutName;
        this.spoutNum = spoutNum;
        this.groupingType = groupingType;
        this.groupingField = groupingField;
    }
    
    public Object getSpout() {
        return spout;
    }
    public void setSpout(Object spout) {
        this.spout = spout;
    }
    public String getSpoutName() {
        return spoutName;
    }
    public void setSpoutName(String spoutName) {
        this.spoutName = spoutName;
    }
    public int getSpoutNum() {
        return spoutNum;
    }
    public void setSpoutNum(int spoutNum) {
        this.spoutNum = spoutNum;
    }
    public String getGroupingType() {
        return groupingType;
    }
    public void setGroupingType(String groupingType) {
        this.groupingType = groupingType;
    }
    public String getGroupingField() {
        return groupingField;
    }
    public void setGroupingField(String groupingField) {
        this.groupingField = groupingField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((groupingField == null) ? 0 : groupingField.hashCode());
        result = prime * result + ((groupingType == null) ? 0 : groupingType.hashCode());
        result = prime * result + ((spout == null) ? 0 : spout.hashCode());
        result = prime * result + ((spoutName == null) ? 0 : spoutName.hashCode());
        result = prime * result + spoutNum;
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
        SpoutInstance other = (SpoutInstance) obj;
        if (groupingField == null) {
            if (other.groupingField != null)
                return false;
        } else if (!groupingField.equals(other.groupingField))
            return false;
        if (groupingType == null) {
            if (other.groupingType != null)
                return false;
        } else if (!groupingType.equals(other.groupingType))
            return false;
        if (spout == null) {
            if (other.spout != null)
                return false;
        } else if (!spout.equals(other.spout))
            return false;
        if (spoutName == null) {
            if (other.spoutName != null)
                return false;
        } else if (!spoutName.equals(other.spoutName))
            return false;
        if (spoutNum != other.spoutNum)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SpoutInstance [spout=" + spout + ", spoutName=" + spoutName + ", spoutNum="
                + spoutNum + ", groupingType=" + groupingType + ", groupingField=" + groupingField
                + "]";
    } 

}
