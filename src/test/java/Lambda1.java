/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/2/28 11:08
 */
public class Lambda1 {
    public static void main(String[] args) {
       /* f1(new Runnable() {
            @Override
            public void run() {
                System.out.println("匿名内部类...");
            }
        });*/
        
        /*f1(() ->
               System.out.println("lambda表达式")
        );*/
    
        // alt+回合
       // f1(() -> System.out.println("匿名内部类..."));
    }
    
    public static void f1(Runnable runnable) {
        runnable.run();
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