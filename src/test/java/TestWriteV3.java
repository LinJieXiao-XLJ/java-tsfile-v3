
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 树模型TsFileV3写入
 */
public class TestWriteV3 {
    // 生成路径
    private static final String path = "TreeTsFileV3.tsfile";
    // 非对齐设备名
    private static String nonAlignedDeviceName = "root.db.v3.d1";

    public static void main(String[] args) {
        try {
            File f = FSFactoryProducer.getFSFactory().getFile(path);
            // 判断文件是否存在
            if (f.exists()) {
                Files.delete(f.toPath());
            }
            // 创建tsFileWriter对象
            TsFileWriter tsFileWriter = new TsFileWriter(f);
            // 写入非对齐数据
            writeNonAligned(tsFileWriter);
            // 关闭文件
            tsFileWriter.close();
            // 查询文件
            testReader();
            System.out.println("文件生成成功");
        } catch (IOException | WriteProcessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 写入非对齐数据
     */
    private static void writeNonAligned(TsFileWriter tsFileWriter) throws WriteProcessException, IOException {
        // 注册非对齐时间序列
        List<MeasurementSchema> schemasNonAligned = new ArrayList<>();
        schemasNonAligned.add(new MeasurementSchema( "m_INT32_", TSDataType.INT32));
        schemasNonAligned.add(new MeasurementSchema( "m_TEXT_", TSDataType.TEXT));
        schemasNonAligned.add(new MeasurementSchema( "m_STRING_", TSDataType.STRING));
        schemasNonAligned.add(new MeasurementSchema( "m_BLOB_", TSDataType.BLOB));
        for (MeasurementSchema schema : schemasNonAligned) {
            tsFileWriter.registerTimeseries(new Path(nonAlignedDeviceName), schema);
        }

        // 生成tablet数据
        Tablet tablet = new Tablet(nonAlignedDeviceName, schemasNonAligned,10);
        tablet.initBitMaps();
        BitMap bitMap = new BitMap(10);
        int row;
        long time = 100L;
        for (int i = 0; i < 10; i++) { // 写入10行
            row = tablet.rowSize++;
            tablet.addTimestamp(row, time++);
            tablet.addValue( "m_INT32_", row, 1);
            // mark null value
            if (i % 2 == 0) {
                tablet.addValue("m_TEXT_", row, "text");
                tablet.addValue("m_STRING_", row, "string");
                tablet.addValue("m_BLOB_", row, new Binary("string".getBytes(StandardCharsets.UTF_8)));
            } else {
                tablet.bitMaps[1].mark(row);
                tablet.bitMaps[2].mark(row);
                tablet.bitMaps[3].mark(row);
            }
        }
        // 写入非对齐时间序列
        tsFileWriter.write(tablet);
    }

    /**
     * 读取数据
     */
    public static void testReader() {
        // 查询非对齐时间序列
        try {
            TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(path);
            TsFileReader reader = new TsFileReader(fileSequenceReader);
            List<Path> selectedSeries = new ArrayList<>();
            selectedSeries.add(new Path(nonAlignedDeviceName, "m_INT32_", true));
            selectedSeries.add(new Path(nonAlignedDeviceName, "m_TEXT_", true));
            QueryDataSet dataSet = reader.query(QueryExpression.create(selectedSeries, null));
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 查询对齐时间序列
//        try {
//            TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(path);
//            TsFileReader reader = new TsFileReader(fileSequenceReader);
//            List<Path> selectedSeries = new ArrayList<>();
//            selectedSeries.add(new Path(nonAlignedDeviceName + "_0_0", measurementName + "_" + "INT32" + "_0_0", true));
//            selectedSeries.add(new Path(alignedDeviceName + "_0_0", measurementName + "_" + "TIMESTAMP" + "_0_0", true));
//            QueryDataSet dataSet = reader.query(QueryExpression.create(selectedSeries, null));
//            while (dataSet.hasNext()) {
//                System.out.println(dataSet.next());
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

}