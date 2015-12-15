/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.jasonstack.cassandra.devops;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDeletedCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;

/**
 * Counts the number of tombstones in a column family folder
 */
public class TombStoneScanner {

    private static final String partitionerName = "Murmur3Partitioner";
    private static List<TombstoneMetric> collector = new ArrayList<>();

    /**
     * java -jar TombStoneScanner.jar [FOLDER]
     * <p>
     * Counts the number of tombstones, per row, in a given SSTable
     * <p>
     * Assumes Murmur3Partitioner, standard columns and UTF8 encoded row keys
     * <p>
     * Does not require a cassandra.yaml file or system tables.
     *
     * @param args command lines arguments
     * @throws java.io.IOException on failure to open/read/write files or output streams
     */
    public static void main(String[] args) throws Exception {
        String usage = String.format("Usage: java -jar %s.jar [FOLDER]", TombStoneScanner.class.getSimpleName());
        args = "/home/zhaoyang/program/cassandra-2.1.8-junit/data/data/system/".split(" ");
        if (args.length < 1) {
            System.err.println("You must supply at least one folder");
            System.err.println(usage);
            System.exit(1);
        }

        buildConfig();

        try {
            Files.walk(Paths.get(args[0])).filter(filePath -> isSSTable(filePath)).forEach(sstableFile -> {
                System.out.println("Scanning SSTable: " + sstableFile);
                Descriptor descriptor = Descriptor.fromFilename(sstableFile.toString());
                try {
                    run(descriptor);
                } catch (Exception e) {
                    System.err.println("Unable to scan SSTable " + sstableFile + ", due to " + e.getMessage());
                }
            });
        } finally {
            process();

            System.exit(0);
        }
    }

    /* Hardcoded SSTable format */
    private static boolean isSSTable(Path filePath) {
        return filePath.getFileName().toString().endsWith("-Data.db");
    }

    /* build necessary config to run scanner */
    private static void buildConfig() throws Exception {
        // Fake DatabaseDescriptor settings so we don't have to load cassandra.yaml etc
        Config.setClientMode(true);

        Field configField = DatabaseDescriptor.class.getDeclaredField("conf");
        configField.setAccessible(true);
        Config config = (Config) configField.get(null);
        config.commitlog_sync = Config.CommitLogSync.batch;
        config.commitlog_sync_batch_window_in_ms = 10.0;
        config.partitioner = partitionerName;
        config.file_cache_size_in_mb = 32;

        String partitionerClassName = String.format("org.apache.cassandra.dht.%s", partitionerName);
        try {
            Class<?> clazz = Class.forName(partitionerClassName);
            IPartitioner partitioner = (IPartitioner) clazz.newInstance();
            DatabaseDescriptor.setPartitioner(partitioner);
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate partitioner " + partitionerClassName);
        }
    }

    /* scan tombstones columns and print */
    private static void run(Descriptor desc) throws IOException {
        CFMetaData cfm = CFMetaData.sparseCFMetaData(desc.ksname, desc.cfname, UTF8Type.instance);
        SSTableReader reader = SSTableReader.open(desc, cfm);
        ISSTableScanner scanner = reader.getScanner();

        long totalTombstones = 0, totalColumns = 0;

        while (scanner.hasNext()) {
            // for each partition
            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

            int tombstonesCount = 0, columnsCount = 0;
            while (row.hasNext()) {
                // for each column in partition
                OnDiskAtom column = row.next();
                if (column instanceof BufferDeletedCell) {
                    tombstonesCount++;
                }
                columnsCount++;
            }
            totalTombstones += tombstonesCount;
            totalColumns += columnsCount;

            if (tombstonesCount > 0) {
                String key;
                try {
                    key = UTF8Type.instance.getString(row.getKey().getKey());
                } catch (RuntimeException e) {
                    key = BytesType.instance.getString(row.getKey().getKey());
                }
                //System.out.printf("PartitionKey: %s, Tombstones(Total): %d (%d)%n", paritionKey, tombstonesCount, columnsCount);
                TombstoneMetric count = new TombstoneMetric(desc.cfname, key, tombstonesCount, columnsCount);
                collector.add(count);
            }
        }
        //System.out.printf("Result: %d tombstones out of total %d columns", totalTombstones, totalColumns);
        scanner.close();
    }

    public static void process() {
		List<TombstoneMetric> result = collector.stream()
				.collect(Collectors.groupingBy(m -> m.table + m.paritionKey))
				.entrySet().stream().map(e -> aggregate(e.getValue()))
				.collect(Collectors.toList());
		Collections.sort(result);
		System.out.println("######################################");
		result.forEach(System.out::println);
    }
    
    private static TombstoneMetric aggregate(List<TombstoneMetric> list){
    	if(list==null||list.isEmpty()){
    		return null;
    	}
    	String table = list.get(0).table;
    	String key = list.get(0).paritionKey;
    	int tombstones = 0;
    	int total =0 ;
    	for(TombstoneMetric metric:list){
    		tombstones+=metric.tombstones;
    		total +=metric.total;
    	} 
    	return new TombstoneMetric(table, key, tombstones, total);
    }

    public static class TombstoneMetric implements Comparable<TombstoneMetric>{
        public String table;
        public String paritionKey;
        public Integer tombstones;
        public Integer total;

        public TombstoneMetric(String table, String key, Integer tombstones, Integer total) {
            this.table = table;
            this.paritionKey = key;
            this.tombstones = tombstones;
            this.total = total;
        }

        @Override
		public String toString() {
			return "TombstoneMetric [table=" + table + ", paritionKey="
					+ paritionKey + ", tombstones=" + tombstones + ", total="
					+ total + "]";
		}

		@Override
		public int compareTo(TombstoneMetric o) { 
			if(this.tombstones!=o.tombstones){
				return this.tombstones- o.tombstones;
			}else{
				return o.total-this.total ;
			} 
		}
    }

    public static Map<String, Integer> sortByValue(Map<String, Integer> map) {
        Map<String, Integer> result = new LinkedHashMap<>();
        Stream<Map.Entry<String, Integer>> st = map.entrySet().stream();
        st.sorted(Comparator.comparing(e -> e.getValue())).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }
}
