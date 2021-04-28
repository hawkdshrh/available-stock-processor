package org.acme;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.acme.beans.Product;
import org.acme.services.AvailableStockService;

@Path("/stock-available")
public class StockResource {

    @Inject
    AvailableStockService availableStockService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<Product,Integer> getAllProductQuantities() {
        return availableStockService.getAllAvailableStock();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("products/{sku}")
    public Integer getQuantity(@PathParam("sku") String sku) {
        return availableStockService.getAvailableStock(new Product(sku));
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("products")
    public Map<Product, Integer> getQuantity() {
        return availableStockService.getAllAvailableStock();
    }
}