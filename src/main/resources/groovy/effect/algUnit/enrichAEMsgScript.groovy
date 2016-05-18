package groovy.effect.algUnit
import java.util.ArrayList;
import java.util.HashMap;
import groovy.transform.CompileStatic

//page_type,args,item_id
@CompileStatic
HashMap<String, Object> manualChangeMsgKVs(Object[] args) {//HashMap<String, Object> oriMsgKVs, String country
    if(args.length != 2)
        return null;

    HashMap<String, Object> oriMsgKVs = (HashMap<String, Object>) args[0];
    String country = (String) args[1];
    oriMsgKVs.remove("page_type");
    oriMsgKVs.remove("args");
    return oriMsgKVs;
}

manualChangeMsgKVs args