package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.*;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ListenableFuture;

public class SendTsBlock {
    private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    private static int fragmentid=0;

    public void send(TsBlock tsBlock) {
//        List<TsBlock> mockTsBlocks = new ArrayList<>();
//        double[] values = new double[] {1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 11.0, 13.0, 15.0, 17.0};
//        double[] values2 = new double[] {4.0, 7.0, 9.0, 10.0, 12.0, 14.0, 16.0, 18.0, 19.0, 20.0};
//        TimeColumn timeColumn2 = new TimeColumn(10, new long[] {4, 7, 9, 10, 12, 14, 16, 18, 19, 20});
//        Column column2 = new DoubleColumn(10, Optional.empty(), values2);
//        TimeColumn timeColumn = new TimeColumn(10, new long[] {1, 2, 3, 5, 6, 8, 11, 13, 15, 17});
//        Column column = new DoubleColumn(10, Optional.empty(), values);
//        TsBlock tsBlock = new TsBlock(timeColumn, column);
//        TsBlock tsBlock2 = new TsBlock(timeColumn2, column2);
//        mockTsBlocks.add(tsBlock);
//        mockTsBlocks.add(tsBlock2);
        final String queryId = "test_query";
//        final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
        final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
        final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentid++, "0");
        final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentid++, "0");
        final String remotePlanNodeId = "receive_test";
        final String localPlanNodeId = "send_test";

        int channelNum = 1;
        AtomicInteger cnt = new AtomicInteger(channelNum);
        long query_num=1;
        FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);

//        ISinkChannel sinkChannel = MPP_DATA_EXCHANGE_MANAGER.createSinkChannel(
//                localFragmentInstanceId,
//                remoteEndpoint,
//                remoteFragmentInstanceId,
//                remotePlanNodeId,
//                localPlanNodeId,
//                instanceContext,
//                cnt);
        DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
        ISinkHandle sinkHandle =
                MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
                        Collections.singletonList(
                                new DownStreamChannelLocation(
                                        remoteEndpoint,
                                        remoteFragmentInstanceId,
                                        remotePlanNodeId)),
                        downStreamChannelIndex,
                        ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                        localFragmentInstanceId,
                        localPlanNodeId,
                        instanceContext);

//        ISourceHandle sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
//                remoteFragmentInstanceId,
//                remotePlanNodeId,
//                0,//IndexOfUpstreamSinkHandle
//                remoteEndpoint,
//                localFragmentInstanceId,
//                instanceContext::failed);

//        sinkChannel.open();
//        sinkChannel.send(tsBlock);
        sinkHandle.tryOpenChannel(0);
        sinkHandle.send(tsBlock);
//        ListenableFuture<?> isBlocked = sourceHandle.isBlocked();
//        while (!isBlocked.isDone()) {
//            try {
////                System.out.println("start to sleep 10");
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        if (!sourceHandle.isFinished()) {
//            TsBlock tsBlock_rev = sourceHandle.receive();
//            sourceHandle.close();
//            Column[] valueColumns = tsBlock_rev.getValueColumns();
//            System.out.println("receive columns:");
//            for(Column valueColumn:valueColumns){
//                System.out.println(valueColumn);
//            }
//            TimeColumn timeColumn_rev=tsBlock_rev.getTimeColumn();
//            long[] times=timeColumn_rev.getTimes();
//            System.out.println("receive time columns:");
//            for(long time:times){
//                System.out.println(time);
//            }
//        }

//        sinkHandle.send(tsBlock);

//        sinkChannel.send(tsBlock2);
//        System.out.println("send1");
//        sourceHandle.isBlocked();
//        System.out.println("block1");


//        sourceHandle.receive();
//        System.out.println("receive1");
//        sinkChannel.send(tsBlock);
//        System.out.println("send2");
//        sinkChannel.setNoMoreTsBlocksOfOneChannel(0);
//        System.out.println("set no more");
//        sourceHandle.receive();
//        System.out.println("receive2");
//        sinkChannel.send(tsBlock);






//        sinkHandle.tryOpenChannel(0);
//        System.out.println("tsBlock need to send:");
//        Column[] valueColumns = tsBlock.getValueColumns();
//        System.out.println(" columns:");
//        for(Column valueColumn:valueColumns){
//            System.out.println(valueColumn);
//        }
//        TimeColumn timeColumn_send=tsBlock.getTimeColumn();
//        long[] times=timeColumn_send.getTimes();
//        System.out.println(" time columns:");
//        for(long time:times){
//            System.out.println(time);
//        }
//        Column[] valueColumns2 = tsBlock2.getValueColumns();
//        System.out.println(" columns2:");
//        for(Column valueColumn:valueColumns2){
//            System.out.println(valueColumn);
//        }
//        TimeColumn timeColumn_send2=tsBlock2.getTimeColumn();
//        long[] times2=timeColumn_send2.getTimes();
//        System.out.println(" time columns2:");
//        for(long time:times2){
//            System.out.println(time);
//        }
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        sinkChannel.isFull();
//        sinkChannel.send(tsBlock);
//        sinkChannel.setNoMoreTsBlocks();
//        sinkChannel.close();
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        sinkChannel.send(mockTsBlocks.get(0));
//        System.out.println("send successfully");
//        try {
//            Thread.sleep(200000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        int numOfMockTsBlock = 1;
//        for (int i = 0; i < numOfMockTsBlock; i++) {
//            try {
//                sinkChannel.getSerializedTsBlock(i);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        System.out.println("getSerializedTsBlock successfully");
////        try {
////            Thread.sleep(10000);
////        } catch (InterruptedException e) {
////            throw new RuntimeException(e);
////        }
//        sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
//        System.out.println("acknowledgeTsBlock successfully");
//
    }
}
