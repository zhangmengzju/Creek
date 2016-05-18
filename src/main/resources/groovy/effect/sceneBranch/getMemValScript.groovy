package groovy.effect.sceneBranch
import java.util.HashMap;

def manualChangeMsgKVs(msgKVs, msgTypePart) {
    if(msgTypePart.equals("metis"))//metis后台的一个tuple对应的是一组item
        return new Long(((String) msgKVs.get("item_list")).split("\004").length);
    else 
        return 1L;
}

manualChangeMsgKVs msgKVs, msgTypePart