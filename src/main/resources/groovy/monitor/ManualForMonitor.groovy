package groovy.monitor
import java.util.HashMap;
import com.alibaba.dt.guider.streaming.framework.producer.monitor.ManualTask;
import com.alibaba.dt.guider.streaming.framework.utils.DateUtils;
import com.alibaba.dt.guider.streaming.framework.utils.Utils;

def manualChangeMsgKVs(msgKVs, country) {
    tupleTime = (String) msgKVs.get("time");
    tupleTimeLong = DateUtils.getTimeLongByCountry("us", tupleTime);
    tupleTimeIn15Min = DateUtils.getTimeIn15MinStringByCountry(country, tupleTimeLong);
    tupleTimeIn15MinLong = DateUtils.getTimeLongByCountry(country, tupleTime);
    tupleTSSegmentId = ((int)(tupleTimeIn15MinLong / ManualTask.TIME_15_MIN_IN_MS)) % ManualTask.RECORD_NUM_IN_48_HOURS;
       
    msgKVs.put("ts", tupleTimeIn15Min);
    msgKVs.put("ts_segment_id", tupleTSSegmentId);

    msgKVs.remove("time");
    return msgKVs;
}

manualChangeMsgKVs msgKVs, country