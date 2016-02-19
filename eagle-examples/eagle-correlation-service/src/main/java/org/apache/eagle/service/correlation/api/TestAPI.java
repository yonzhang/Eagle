package org.apache.eagle.service.correlation.api;

import javax.ws.rs.*;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yonzhang on 2/18/16.
 */
@Path("/metric")
public class TestAPI {
    @GET
    @Path("/list")
    @Produces({"application/json"})
    public String getMetrics(){
        return "metric1";
    }
}
