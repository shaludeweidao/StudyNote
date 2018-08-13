package bean;

public class Bean_1 {
    String name;
    Integer age;
    String[] favorite;


    public Bean_1(String name, Integer age, String[] favorite) {
        this.name = name;
        this.age = age;
        this.favorite = favorite;
    }

    public Bean_1() {
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String[] getFavorite() {
        return favorite;
    }

    public void setFavorite(String[] favorite) {
        this.favorite = favorite;
    }
}
