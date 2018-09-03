package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

//jdbc
public class Demo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        String url = "jdbc:mysql://localhost:3306/spark";
        String user = "root";
        String password = "root";

        //1. 注册驱动   不同的数据库,有不同的Driver实现, 本质上java其实就是提供了借口,各个厂商去实现这些接口
        Class.forName("com.mysql.jdbc.Driver");


        //2. 创建连接   jdbc协议
        final Connection conn = DriverManager.getConnection(url, user, password);


        //3. 创建传输的小车    用于发送sql
        final Statement st = conn.createStatement();


        //4. 执行sql语句
        String sql_createTable =
                "CREATE TABLE if not exists t_netflow_app_magicisland ( \n" +
                "stat_date date comment '统计日期', \n" +
                "DAU  int(12) DEFAULT NULL    COMMENT '区块链DAU', \n" +
                "retention  double(20,4) DEFAULT '0.0000' COMMENT '区块链活跃用户次日留存率', \n" +
                "new_user  int(12) DEFAULT NULL    COMMENT '区块链新用户', \n" +
                "newuser_retention  double(20,4) DEFAULT '0.0000' COMMENT '区块链新用户次日留存率', \n" +
                "mining_user  int(12) DEFAULT NULL    COMMENT '挖矿人数', \n" +
                "mining_user_rate  double(20,4) DEFAULT '0.0000' COMMENT '挖矿率', \n" +
                " \n" +
                "inviter_ore  int(12) DEFAULT NULL    COMMENT '邀请人数发矿人数（成功邀请）', \n" +
                "inviter  int(12) DEFAULT NULL    COMMENT '邀请参与人数', \n" +
                "inviter_rate  double(20,4) DEFAULT '0.0000' COMMENT '邀请人数参与率', \n" +
                "complete_invt_rate  double(20,4) DEFAULT '0.0000' COMMENT '邀请完成率', \n" +
                "inviter_retention  double(20,4) DEFAULT '0.0000' COMMENT '邀请人数留存率', \n" +
                "inviter_50  int(12) DEFAULT NULL    COMMENT '邀请人数50人', \n" +
                "inviter_50_rate  double(20,4) DEFAULT '0.0000' COMMENT '邀请人数50人占比', \n" +
                "poster_show  int(12) DEFAULT NULL    COMMENT '传播海报展示量', \n" +
                "invitee  int(12) DEFAULT NULL    COMMENT '被邀请人数', \n" +
                " \n" +
                "sharing_ore  int(12) DEFAULT NULL    COMMENT '分享帖子发矿人数（分享成功）', \n" +
                "sharing  int(12) DEFAULT NULL    COMMENT '分享帖子参与人数', \n" +
                "sharing_rate  double(20,4) DEFAULT '0.0000' COMMENT '分享帖子参与率', \n" +
                "complete_sharing_rate  double(20,4) DEFAULT '0.0000' COMMENT '分享帖子完成率', \n" +
                "sharing_retention  double(20,4) DEFAULT '0.0000' COMMENT '每日帖子分享留存率', \n" +
                "sharing_5  int(12) DEFAULT NULL    COMMENT '每日分享5条人数', \n" +
                "sharing_5_rate  double(20,4) DEFAULT '0.0000' COMMENT '每日分享5条人数占比', \n" +
                " \n" +
                "ore_dist  int(12) DEFAULT NULL    COMMENT '矿石发放总量', \n" +
                "ore_obtain_user  int(12) DEFAULT NULL    COMMENT '每日获得矿石人数', \n" +
                "ore_invt_obtain  int(12) DEFAULT NULL    COMMENT '邀请获得矿石量',   \n" +
                "ore_share_obtain  int(12) DEFAULT NULL    COMMENT '分享帖子获得矿石量',  \n" +
                "ore_recycle  int(12) DEFAULT NULL    COMMENT '每日矿石回收总量'  \n" +
                ") ENGINE = MyISAM  DEFAULT CHARSET=utf8  COMMENT '育荣,神奇岛挖矿数据报表' " ;
        st.execute(sql_createTable);
        String sql_update = "update t_netflow_app_magicisland   set DAU=1234 , retention = 2.2222222   where  stat_date = '20180901'";

        st.execute(sql_update);


        //5. 关闭连接
        conn.close();
        System.out.println("end");
    }

}
