package groovy.effect.algUnit
import java.util.HashMap;
import groovy.transform.CompileStatic
@CompileStatic
long manualChangeMsgKVs(Object[] args) {
    if(args.length != 2)
        return 0L;

    HashMap<String, Object> msgKVs = (HashMap<String, Object>) args[0];
    String msgTypePart = (String)args[1];
    
    return 1L;
}

manualChangeMsgKVs args