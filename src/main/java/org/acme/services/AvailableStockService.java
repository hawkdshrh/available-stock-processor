package org.acme.services;

import io.quarkus.runtime.StartupEvent;
import org.acme.beans.Product;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.acme.topology.AvailableStockTopologyProducer.STOCK_AVAILABLE_KEYSTORE;

@ApplicationScoped
public class AvailableStockService {
    
    private static final Logger LOGGER = Logger.getLogger("AvailableStockService");

    @Inject
    KafkaStreams streams;

    private ReadOnlyKeyValueStore<Product,Integer> productQuantityKeystore;

    void onStart(@Observes StartupEvent ev) {
        productQuantityKeystore = getKeyStore(streams, STOCK_AVAILABLE_KEYSTORE);
    }

    private static ReadOnlyKeyValueStore<Product,Integer> getKeyStore(final KafkaStreams streams, final String name) {
        while (true) {
            try {
                final StoreQueryParameters<ReadOnlyKeyValueStore<Product,Integer>> storeQueryParameters = StoreQueryParameters
                        .fromNameAndType(name, QueryableStoreTypes.keyValueStore());

                return streams.store(storeQueryParameters);
            } catch (InvalidStateStoreException|IllegalStateException e) {
                // ignore, store not ready yet
            }
        }
    }

    public Map<Product,Integer> getAllAvailableStock() {
        if(productQuantityKeystore != null) {
            final KeyValueIterator<Product,Integer> productStock = productQuantityKeystore.all();
            final Stream<KeyValue<Product,Integer>> productStockStream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(productStock, Spliterator.ORDERED), false);
            final Map<Product,Integer> map = productStockStream.collect(
                    Collectors.toMap(k -> k.key, k -> k.value));
            productStock.close();

            return map;
        } else {
            return null;
        }
    }

    public Integer getAvailableStock(Product product) {
        
        if (product == null) {
            LOGGER.info("No valid product provided"); 
            return 0;
        }
        
        if (productQuantityKeystore != null) {
            
            final KeyValueIterator<Product,Integer> productStock = productQuantityKeystore.all();
            final Stream<KeyValue<Product,Integer>> productStockStream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(productStock, Spliterator.ORDERED), false);
            final Map<Product,Integer> map = productStockStream.collect(
                    Collectors.toMap(k -> k.key, k -> k.value));
            productStock.close();
            
            LOGGER.log(Level.INFO, "Found keystore; looking up product for {0}", product.getProductSku()); 
            if(map.containsKey(product)) {
                LOGGER.info("Found product"); 
                return map.get(product);
            } else {
                LOGGER.info("No matching product found"); 
                return 0;
            }
        } else {
            LOGGER.info("Keystore not found"); 
            return 0;
        }
    }
}
