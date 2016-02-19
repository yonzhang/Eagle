package org.apache.eagle.service.correlation.api;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.ws.rs.*;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yonzhang on 2/18/16.
 */
@Path("/")
public class TestAPI {
    @GET
    @Path("/metrics")
    @Produces({"application/json"})
    public String getMetrics(){
        return "metric1";
    }

    @GET
    @Path("/groups")
    @Produces({"application/json"})
    public Response getMetricGroups(){
        Response r = new Response();
        r.group = new ArrayList<String>();
        r.group.add("group1");
        return r;
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(propOrder = {})
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class Response{
        public List<String> group;
    }
}
