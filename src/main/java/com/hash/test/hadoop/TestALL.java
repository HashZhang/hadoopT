package com.hash.test.hadoop;

import java.util.StringTokenizer;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/20
 */
public class TestALL {
    public static void main(String[] args) {
        System.out.println("默认以空格，\\t,\\r,\\n分割");
        StringTokenizer st = new StringTokenizer("www ooobj com");
        while(st.hasMoreElements()){
            System.out.println("Token:" + st.nextToken());
        }
        System.out.println("指定以.分割");
        st = new StringTokenizer("www.ooobj.com",".");
        while(st.hasMoreElements()){
            System.out.println("Token:" + st.nextToken());
        }
        System.out.println("指定以.分割,并在结果中包含分隔符");
        st = new StringTokenizer("www.ooobj.com",".",true);
        while(st.hasMoreElements()){
            System.out.println("Token:" + st.nextToken());
        }
    }
}
