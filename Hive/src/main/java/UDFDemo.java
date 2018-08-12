import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFDemo  extends UDF{

    public String evaluate( String key){
        if (key == null){

            return "0";
        }

        return key.length()+"";
    }




    //main  方法只是测试此udf 是否正确
    public static void main(String[] args) {

        System.out.println(new UDFDemo().evaluate("abc"));
    }
}
