package main.java.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: echirivella
 * Date: 10/18/13
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class Hbasecons {
    private HTable table;
    private Configuration config;
    private String study;



    public Hbasecons(String db, String studio){
        study = studio;
        config = HBaseConfiguration.create();
        config.set("hbase.master", "172.24.79.30:60010");
        config.set("hbase.zookeeper.quorum", "172.24.79.30");
        config.set("hbase.zookeeper.property.clientPort","2181");
        try{
            table = new HTable(config, db);
        }catch (IOException e){
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }


    public String sample(String rowkey, String sample) {
        Get get = new Get(rowkey.getBytes());
        QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(sample));
        get.setFilter(filter);
        try{
            StringBuilder result = new StringBuilder();
            result.append("Position: ");
            String[] position = rowkey.split("_");
            result.append("chromosome");
            result.append(position[0]);
            result.append(" position: ");
            result.append(position[1]);
            result.append("</br>");
            result.append("       Sample: ");
            result.append(sample);
            Result res = table.get(get);
            VariantFieldsProtos.VariantSample variantSample = VariantFieldsProtos.VariantSample.parseFrom(res.getValue("d".getBytes(), (study + "_" + sample).getBytes()));
            for(int i=0;i<variantSample.getFieldsList().size();i++){
                 result.append(variantSample.getFields(i).getKey().toString());
                 result.append(": ");
                 result.append(variantSample.getFields(i).getValue().toString());
            }
            return result.toString();
        }catch (IOException e){
            StringBuilder result = new StringBuilder();
            result.append(e.getStackTrace());
            result.append(e.getClass().getName() + ": " + e.getMessage());
            return result.toString();
        }
    }
}
