package groovy
import com.alibaba.dt.guider.streaming.framework.utils.Utils;

def run(tmpId) {
    
    Utils ut = new Utils();
    ut.hello();
    println 'consumer tmpId:'
    res = tmpId * 10
    
    
    res
}

run tmpId