package org.apache.iotdb;

import org.apache.iotdb.tree.TreeTsFileV3_NoSameDevice_NoSameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV3_NoSameDevice_SameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV3_SameDevice_NoSameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV3_SameDevice_SameTimeStamp;

public class Main {
    public static void main(String[] args) {
        TreeTsFileV3_NoSameDevice_NoSameTimeStamp t1 = new TreeTsFileV3_NoSameDevice_NoSameTimeStamp();
        t1.testWrite();
        t1.testReader();

        TreeTsFileV3_NoSameDevice_SameTimeStamp t2 = new TreeTsFileV3_NoSameDevice_SameTimeStamp();
        t2.testWrite();
        t2.testReader();

        TreeTsFileV3_SameDevice_NoSameTimeStamp t3 = new TreeTsFileV3_SameDevice_NoSameTimeStamp();
        t3.testWrite();
        t3.testReader();

        TreeTsFileV3_SameDevice_SameTimeStamp t4 = new TreeTsFileV3_SameDevice_SameTimeStamp();
        t4.testWrite();
        t4.testReader();
    }
}