import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class UDTFDemo extends GenericUDTF {
    private PrimitiveObjectInspector stringOI = null;

    //初始化环境
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        //构建字段名称, 可以设置多个
        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("字段a_名称");
        fieldNames.add("字段b_名称");

        //构建字段类型
        ArrayList<ObjectInspector> types = new ArrayList();
        types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //将字段名称和字段类型输出
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,types);
    }


    //具体的执行过程
    @Override
    public void process(Object[] args) throws HiveException {

        char[] chars = args[0].toString().toCharArray();
        for (char c :chars){
            String[] strs = {""+c, ""+c+c+c};
            //forward方法将每行数据输出
            forward(strs);
        }
    }


    //最后的操作,可以不用写
    @Override
    public void close() throws HiveException {

    }




    public static void main(String[] args) throws HiveException {
        UDTFDemo myUDTF = new UDTFDemo();

        Object[] objects = {"abc"};
        myUDTF.process(objects);
    }
}
