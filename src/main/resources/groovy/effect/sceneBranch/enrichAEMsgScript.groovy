package groovy.effect.sceneBranch
import java.util.HashMap;

def manualChangeMsgKVs(msgKVs, country) { 
    requestId = ((String) msgKVs.get("args")).split("_rid=")[1].split(",")[0];
    msgKVs.put("request_id", requestId);
    msgKVs.remove("args");
    
    return msgKVs;
}

manualChangeMsgKVs msgKVs, country  

