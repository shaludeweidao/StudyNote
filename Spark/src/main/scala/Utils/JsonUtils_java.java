package Utils;

import bean.Bean_1;
import com.alibaba.fastjson.JSON;

import java.util.List;

public class JsonUtils_java {
    public static void main(String[] args) {
        final Bean_1 bean1 = new Bean_1("a", 1, new String[]{"aa", "bb"});
        final Bean_1 bean2 = new Bean_1("a", 1, new String[]{"aa", "bb"});
        final String str = JSON.toJSONString(new Bean_1[]{bean1,bean2});
        System.out.println(str);



        String jsonString = "{'name':'abc','age':2}";
        final Bean_1 bean3 = JSON.parseObject(jsonString, Bean_1.class);

        String arrJson = "[{\"age\":1,\"favorite\":[\"aa\",\"bb\"],\"name\":\"a\"},{\"age\":1,\"favorite\":[\"aa\",\"bb\"],\"name\":\"a\"}]";
        final List<Bean_1> bean_1s = JSON.parseArray(arrJson, Bean_1.class);
        System.out.println(bean_1s.size());


    }

}
