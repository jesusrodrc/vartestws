package main.java;

import main.java.lib.Hbasecons;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: echirivella
 * Date: 10/17/13
 * Time: 6:02 PM
 * To change this template use File | Settings | File Templates.
 */

@Path("/show")
public class vartestws {
    @GET
    @Path("/info1000/{db}/")
    public String info1000() throws IOException {
        StringBuilder resultado = new StringBuilder();
        return resultado.toString();
    }


    @GET
    @Path("/vcfrecord1000/{db}/{study}/{var}/{sp}")
    public String vcfrecord(@PathParam("db") String db, @PathParam("study") String study,@PathParam("var") String var, @PathParam("sp") String sp) throws IOException{
        StringBuilder resultado = new StringBuilder();
        Hbasecons prueba = new Hbasecons(db, study);
        resultado.append(prueba.sample(var, sp));
        return resultado.toString();
    }
}
