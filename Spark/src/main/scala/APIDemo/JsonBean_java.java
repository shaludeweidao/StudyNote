package APIDemo;

import scala.Product;
import scala.Serializable;
import scala.collection.Iterator;

public class JsonBean_java {
    String dt = null;
    String key = null;
    String value = null;
    String meaning = null;


    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMeaning() {
        return meaning;
    }

    public void setMeaning(String meaning) {
        this.meaning = meaning;
    }
}
