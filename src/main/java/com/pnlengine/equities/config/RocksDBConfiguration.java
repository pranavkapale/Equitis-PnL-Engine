package com.pnlengine.equities.config;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;

import java.util.Map;

public class RocksDBConfiguration implements RocksDBConfigSetter {

    // E.g. cap at 256MB per block cache
    private static final long BLOCK_CACHE_SIZE = 256 * 1024 * 1024L;

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        
        // Use a block-based table configuration
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        
        // 1. Cap off-heap memory usage via Block Cache
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        
        // 2. Enable Bloom filters to optimize disk reads
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        
        options.setTableFormatConfig(tableConfig);

        // 3. Optimize write buffers
        options.setWriteBufferSize(64 * 1024 * 1024L); // 64MB per write buffer
        options.setMaxWriteBufferNumber(3);
    }

    @Override
    public void close(final String storeName, final Options options) {
        // RocksDB Config Setter should typically close the Options/TableConfig 
        // to prevent native memory leaks when streams are shut down
        // However, Kafka Streams generally manages `Options` object's lifecycle.
        // It's important to not accidentally close options that Kafka Streams might still be using.
    }
}
