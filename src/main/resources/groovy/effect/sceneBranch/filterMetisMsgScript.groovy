package groovy.effect.sceneBranch
import java.util.HashMap;

def manualChangeMsgKVs(msgKVs) { 
    itemList = (String) msgKVs.get("item_list");
    if (itemList != null && itemList.length() != 0)
        return true;
    else
        return false;
}

manualChangeMsgKVs msgKVs  

