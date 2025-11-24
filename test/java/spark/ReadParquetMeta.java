import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ReadParquetMeta {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(args[0]);
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata meta = reader.getFooter();
            
            System.out.println("=== Parquet File Metadata ===");
            System.out.println("Blocks (row groups): " + meta.getBlocks().size());
            System.out.println("File size from footer: " + inputFile.getLength());
            System.out.println("");
            
            meta.getBlocks().forEach(block -> {
                System.out.println("Row Group:");
                System.out.println("  Rows: " + block.getRowCount());
                System.out.println("  Total byte size: " + block.getTotalByteSize());
                System.out.println("  Columns: " + block.getColumns().size());
                System.out.println("");
                
                block.getColumns().forEach(col -> {
                    System.out.println("  Column: " + col.getPath());
                    System.out.println("    First data page offset: " + col.getFirstDataPageOffset());
                    System.out.println("    Dictionary page offset: " + col.getDictionaryPageOffset());
                    System.out.println("    Total size: " + col.getTotalSize());
                    System.out.println("    Total uncompressed size: " + col.getTotalUncompressedSize());
                    System.out.println("");
                });
            });
        }
    }
}
