/**
 * @author Aaron
 * @date 2022/6/23 11:08
 */

public class staticTest {
    private static int a = 1;

    public static void A(){
        a = 2;
        System.out.println("a = "+a);
    }

    public static void B(){
        a = a+1;
        System.out.println("a = "+a);
    }
}
