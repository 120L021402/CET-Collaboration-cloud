package org.apache.iotdb.db.queryengine.plan.execution;


import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.DataNode;
import org.apache.iotdb.db.zcy.service.CtoEService;
import org.apache.iotdb.db.zcy.service.TSInfo;
public class ServiceImpl implements CtoEService.Iface{
    private TSInfo message;

    public ServiceImpl() {
        this.message = new TSInfo(0, 0, 0, 0);
    }
    @Override
    public void sendData(TSInfo data) {
        // 实现 sendData 方法的逻辑
        try {
            System.out.println("Sending data: " + data);
            message.setSize(data.getSize());
            message.setNum(data.getNum());
            message.setMin(data.getMin());
            message.setMax(data.getMax());
            Thread thread_send = new Thread(new testRunnable(data.getNum()));//发送数据测试
            thread_send.start();
        } catch (Exception e) {
            System.err.println("Error sending data: " + e.getMessage());
        }

    }
    @Override
    public TSInfo receiveData() {
        // 实现 receiveData 方法的逻辑
        try {
            System.out.println("Received data: " + message);
//            Thread thread_send = new Thread(new testRunnable());//发送数据测试
//            thread_send.start();
            return message;
        } catch (Exception e) {
            System.err.println("Error receiving data: " + e.getMessage());
            return null;
        }
    }


}
class testRunnable implements Runnable {
    private int fragmentid;
    public testRunnable(int num){
        fragmentid=num;
    }
    @Override
    public void run() {

//        String sql1="SELECT * FROM root.ln.wf02.wt02";
        String sql2="SELECT * FROM root.ln.wf02.wt02 WHERE timestamp!=4";
        ClientRPCServiceImpl clientRPCService = new ClientRPCServiceImpl();
//        clientRPCService.excuteIdentitySql(sql1,fragmentid);
        clientRPCService.excuteIdentitySql(sql2);


    }
}
