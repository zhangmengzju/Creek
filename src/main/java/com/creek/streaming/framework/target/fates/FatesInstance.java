package com.creek.streaming.framework.target.fates;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dt.fates.common.EntityStore;
import com.alibaba.dt.fates.common.FatesCommonsLocator;
import com.alibaba.dt.fates.common.FatesDataOperator;
import com.alibaba.dt.fates.common.dto.StoreFieldValue;
import com.creek.streaming.framework.store.hbase.HTableInstance;
import com.creek.streaming.framework.utils.MD5Utils;
import com.creek.streaming.framework.utils.SelectionSortUtils;
import com.creek.streaming.framework.utils.SelectionSortUtils.KVPair;
import com.google.common.collect.Lists;

public class FatesInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(FatesInstance.class);

    private HTableInstance      hti;
    private FatesDataOperator   dataOperator;

    public FatesInstance(HTableInstance htiInput, FatesDataOperator dataOperatorInput) {
        hti = htiInput;
        dataOperator = dataOperatorInput;
    }

    public void putSimple(String str, String subject, String field, String memberSeq) {
        ///DataType:simple
        EntityStore store = dataOperator.queryEntityData(subject, memberSeq);
        store.putFieldValue(field, new StoreFieldValue().newSimple(str));
        dataOperator.updateEntityData(subject, memberSeq, store);
    }

    public void putList(List<String> list, String subject, String field, String memberSeq) {
        ///DataType:list
        EntityStore store = dataOperator.queryEntityData(subject, memberSeq);
        store.putFieldValue(field, new StoreFieldValue().newList(list));
        dataOperator.updateEntityData(subject, memberSeq, store);
    }

    public void putMap(Map<Long, Long> map, String subject, String field, String memberSeq) {
        ///DataType:map
        try {
            EntityStore store = dataOperator.queryEntityData(subject, memberSeq);
            store.putFieldValue(field, new StoreFieldValue().newMap(map));
            dataOperator.updateEntityData(subject, memberSeq, store);
        } catch (Throwable t) {

            /////-----
            EntityStore store = dataOperator.queryEntityData(subject, memberSeq);
            store.putFieldValue(field, new StoreFieldValue().newMap(map));
            dataOperator.updateEntityData(subject, memberSeq, store);
            LOGGER.warn(String.format("[insertIntoFates retry OK][memberSeq: %s]",
                    memberSeq.toString()));
        }
        //EntityStore storeNew = dataOperator.queryEntityData("ae_buyer", memberSeq + "");
        //LOGGER.warn(String.format("[insertIntoFates][memberSeq: %s][store] %s", memberSeq.toString(),
        //        storeNew.getFieldValue("categoryTop5")));
    }

    public void hbaseDataToFates(Long memberSeq) {

        String prefix = null;
        try {
            prefix = MD5Utils.md5(memberSeq.toString()).substring(0, 4) + memberSeq;
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("MD5 error.", e);
            return;
        }

        Iterator<Result> rsIter = hti.scan(prefix).iterator();

        if (rsIter == null || rsIter.hasNext() == false) {
            LOGGER.debug(String.format(
                    "[Fates insertIntoFates scan hbase but get nothing][memberSeq: %d]", memberSeq));
            
            putMap(new HashMap<Long, Long>(), "ae_buyer", "categoryTop5", String.valueOf(memberSeq));
        } else {
            //KVPair3个成员变量
            //key:fe4e3545240223702464562100002861 => MD5(memberSeq).substring(0,4) + memberSeq + categoryId
            //timestamp:1441893115660
            //value:1
            List<Result> rsList = Lists.newArrayList(rsIter);
            Result[] rsArr = new Result[rsList.size()];
            rsList.toArray(rsArr);

            int topK = rsArr.length < 5 ? rsArr.length : 5;//categoryCount的前5个或者更少
            KVPair[] kvTop5 = SelectionSortUtils.selectSortTopK(rsArr, topK);

            ////Put map to Fates
            Map<Long, Long> map = new HashMap<Long, Long>();
            for (int i = 0; i < topK; i++) {
                long categoryId = Long.valueOf(kvTop5[i].key.replaceFirst(prefix, ""));
                long categoryCount = kvTop5[i].value;
                map.put(categoryId, categoryCount);
            }

            putMap(map, "ae_buyer", "categoryTop5", String.valueOf(memberSeq));
        }
        ////----
        //EntityStore store = dataOperator.queryEntityData("ae_buyer", memberSeq + "");
        //LOGGER.warn(String.format("[insertIntoFates][memberSeq: %s][store] %s",
        //        memberSeq.toString(), store.getFieldValue("categoryTop5")));
    }

    public void hbaseDataToFates(HashSet<Long> activeMemberSeqsSet) {
        for (Long memberSeq : activeMemberSeqsSet) {

            String prefix = null;
            try {
                prefix = MD5Utils.md5(memberSeq.toString()).substring(0, 4) + memberSeq;
            } catch (NoSuchAlgorithmException e) {
                LOGGER.error("MD5 error.", e);
                return;
            }

            Iterator<Result> rsIter = hti.scan(prefix).iterator();

            if (rsIter == null || rsIter.hasNext() == false) {
                LOGGER.warn(String.format(
                        "[Fates insertIntoFates scan hbase but get nothing][memberSeq: %d]",
                        memberSeq));
                return;
            }

            List<Result> rsList = Lists.newArrayList(rsIter);

            //KVPair3个成员变量
            //key:fe4e3545240223702464562100002861 => MD5(memberSeq).substring(0,4) + memberSeq + categoryId
            //timestamp:1441893115660
            //value:1
            Result[] rsArr = new Result[rsList.size()];
            rsList.toArray(rsArr);

            int topK = rsArr.length < 5 ? rsArr.length : 5;//categoryCount的前5个或者更少
            KVPair[] kvTop5 = SelectionSortUtils.selectSortTopK(rsArr, topK);

            ////Put map to Fates
            Map<Long, Long> map = new HashMap<Long, Long>();
            for (int i = 0; i < topK; i++) {
                long categoryId = Long.valueOf(kvTop5[i].key.replaceFirst(prefix, ""));
                long categoryCount = kvTop5[i].value;
                map.put(categoryId, categoryCount);
            }

            putMap(map, "ae_buyer", "categoryTop5", String.valueOf(memberSeq));
            //EntityStore store = dataOperator.queryEntityData("ae_buyer", memberSeq + "");
            //LOGGER.debug(String.format("[insertIntoFates][memberSeq: %s][store] %s", memberSeq.toString(),
            //       store.getFieldValue("categoryTop5")));
        }
        LOGGER.warn(String.format("[insertIntoFates][memberSeq] %s", activeMemberSeqsSet.toString()));
    }

    public static void main(String[] args) {
        FatesDataOperator fatesDataOp = FatesCommonsLocator.getDataOperator();

        ////Put map
        //		Map<String, String> map = new HashMap<String, String>();
        //		map.put("-111L", "-112L");
        //		//Map<String, String> map, String subject, String field, String member_seq
        //		putMap(map, "ae_buyer", "categoryTop5", "memberId1234");

        //Put list

        EntityStore store1 = fatesDataOp.queryEntityData("ae_buyer", "123456789");
        System.out.println("[1]" + store1.getFieldValue("categoryTop5"));

        List<String> list1 = new ArrayList<String>();
        list1.add("-111L");
        list1.add("-222L");
        store1.putFieldValue("categoryTop5", new StoreFieldValue().newList(list1));
        fatesDataOp.updateEntityData("ae_buyer", "123456789", store1);

        EntityStore store2 = fatesDataOp.queryEntityData("ae_buyer", "123456789");
        System.out.println("[2]" + store2.getFieldValue("categoryTop5"));

        List<String> list2 = new ArrayList<String>();
        list2.add("-333L");
        list2.add("-444L");
        store2.putFieldValue("categoryTop5", new StoreFieldValue().newList(list2));
        fatesDataOp.updateEntityData("ae_buyer", "123456789", store2);

        EntityStore store3 = fatesDataOp.queryEntityData("ae_buyer", "123456789");
        System.out.println("[3]" + store3.getFieldValue("categoryTop5"));

        ////Put simple
        //		String str = "newValue";
        //		//String str, String subject, String field, String member_seq
        //		putSimple(str, "ae_buyer", "categoryTop5", "memberId1234");

        System.out.println("exit");
        System.exit(0);
    }

}
