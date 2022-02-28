/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/2/28 11:08
 */
public class Lambda2 {
    public static void main(String[] args) {
        /*f1(new My() {
            @Override
            public String key(User user) {
                return user.getName();
            }
        });*/
        
        //        f1(user -> user.getName());
        f1(User::getName);  // 方法引用
    }
    
    public static void f1(My my) {
        String n = my.key(new User("李四"));
        System.out.println(n);
    }
    
}

interface My {
    String key(User user);
}

class User {
    private String name;
    
    public User(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}
/*

接口:
    1.8之前   常量+抽象方法
    1.8开始:  常量+抽象方法+默认方法
什么时候可以使用lambda表达式:
 
 如果一个方法接受一个接口, 并且这个接口只有一个抽象方法,则可以使用
 lambda表达式来代替接口实现类的对象
 
方法引用

 */