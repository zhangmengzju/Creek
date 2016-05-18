package groovy.effect.algUnit
import java.util.HashMap;
import groovy.transform.CompileStatic
//time,scene_id,request_id,scene_branch,item_list,exec_unit_id
@CompileStatic
boolean manualChangeMsgKVs(Object[] args) { 
    if(args.length != 1)
        return false;

    HashMap<String, Object> msgKVs = (HashMap<String, Object>) args[0];
 
    String itemList = (String) msgKVs.get("item_list");
    if (itemList != null && itemList.length() != 0)
        return true;
    else
        return false;
}

manualChangeMsgKVs args  

