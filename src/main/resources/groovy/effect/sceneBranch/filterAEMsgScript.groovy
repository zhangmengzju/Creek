package groovy.effect.sceneBranch
import java.util.HashMap;

def manualChangeMsgKVs(msgKVs) { 
     pageType = (String) msgKVs.get("page_type");
     args = (String) msgKVs.get("args");
     if (pageType.equals("DETAIL") && args.contains("_rid")
                && (args.contains("_rid=N/A") == false))
            return true;
     else
            return false;
}

manualChangeMsgKVs msgKVs  

