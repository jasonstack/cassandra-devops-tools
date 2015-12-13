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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.*;

import java.nio.file.*;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Counts the number of tombstones in a column family folder
 */
public class TomnStoneScanner {

	private static final String partitionerName = "Murmur3Partitioner";

	/**
	 * java -jar TomnStoneScanner.jar [FOLDER]
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
		String usage = String.format("Usage: java -jar %s.jar [FOLDER]", TomnStoneScanner.class.getSimpleName());

		if (args.length < 1) {
			System.err.println("You must supply at least one folder");
			System.err.println(usage);
			System.exit(1);
		}

		buildConfig();

		Files.walk(Paths.get(args[0])).filter(filePath -> isSSTable(filePath)).forEach(sstableFile -> {
			System.out.println("Scanning SSTable: " + sstableFile);
			Descriptor descriptor = Descriptor.fromFilename(sstableFile.toString());
			try {
				run(descriptor);
			} catch (IOException e) {
				System.err.println("Unable to scan SSTable " + sstableFile + ", due to " + e.getMessage());
			}
		});

		System.exit(0);
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

			if (tombstonesCount >= 0) {
				String key;
				try {
					key = UTF8Type.instance.getString(row.getKey().getKey());
				} catch (RuntimeException e) {
					key = BytesType.instance.getString(row.getKey().getKey());
				}
				System.out.printf("PartitionKey: %s, Tombstones(Total): %d (%d)%n", key, tombstonesCount, columnsCount);
			}

		}
		System.out.printf("Result: %d tombstones out of total %d columns", totalTombstones, totalColumns);

		scanner.close();
	}

}
