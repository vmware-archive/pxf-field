package com.pivotal.pxf.plugins.dram;

/**
 * Created by kimm5 on 8/13/15.
 */
public class Utils {

    public  static String byteToBits(byte[]  v){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%8s", Integer.toBinaryString(v[0] & 0xFF)).replace(' ','0'));
        sb.append("-");
        sb.append(String.format("%8s", Integer.toBinaryString(v[1] & 0xFF)).replace(' ', '0'));
        sb.append("-");
        sb.append(String.format("%8s", Integer.toBinaryString(v[2] & 0xFF)).replace(' ', '0'));
        sb.append("-");
        sb.append(String.format("%8s", Integer.toBinaryString(v[3] & 0xFF)).replace(' ', '0'));
        return sb.toString();
    }

    /**
     * 자바에서 사용하는 Big-Endian 정수값을 Little-Endian 바이트 배열로 변환한다.
     */
    public static byte[] getBigEndian(byte[] v){
        byte[] buf = new byte[4];
        buf[3] = v[0];
        buf[2] = v[1];
        buf[1] = v[2];
        buf[0] = v[3];
        return buf;
    }
}
