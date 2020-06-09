package org.acme;

import org.acme.domain.Document;
import org.acme.service.ExampleService;
import org.jboss.resteasy.annotations.jaxrs.PathParam;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/hello")
public class ExampleResource {

    @Inject
    ExampleService service;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/greeting/{name}")
    public String greeting(@PathParam("name") String name) {
        return service.greet(name);
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "hello";
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/doc/{name}")
    public String getDocument(@PathParam("name") String id){
        Flux<Document> doc = service.findByName(id);
        return doc.toString();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/doc/create")
    public void createDocument(){
        Document doc = new Document();
        doc.setId("1");
        doc.setName("name");
        doc.setDescription("this is a test");
        doc.setVersion(1L);
        service.save(doc);
    }
}