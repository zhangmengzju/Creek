package groovy.effect.algUnit
import java.util.HashMap;
import groovy.transform.CompileStatic
//page_type,args,item_id
@CompileStatic
boolean manualChangeMsgKVs(Object[] args) {
    if(args.length != 1)
        return false;
    
     HashMap<String, Object> msgKVs = (HashMap<String, Object>) args[0];
     
     String pageType = (String) msgKVs.get("page_type");
     String argsInMsgKVs = (String) msgKVs.get("args");
     String itemId = (String) msgKVs.get("item_id");
     if (itemId != null && pageType.equals("DETAIL") && 
         argsInMsgKVs.contains("_rid") && (argsInMsgKVs.contains("_rid=N/A") == false))
            return true;
     else
            return false;
}

manualChangeMsgKVs args  

