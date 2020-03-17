package com.test;

public class test_01 {
    public static void main(String[] args) {
        String student = "liaomj,aaa,bbb";
        String[] tuple = student.split(",");
        String a = tuple[0];
        String b = tuple[1];
        System.out.println(a+","+b);
    }
}
