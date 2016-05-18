package groovy
import com.alibaba.dt.guider.streaming.framework.utils.Utils;

def run(producerId) {
    Utils ut = new Utils();
    ut.insertQueue(producerId);
}

run producerId