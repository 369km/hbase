package com.dx.controller;

import com.dx.hbase.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Description
 * @Date 2020/11/26 下午3:32
 * @Created by yangfudong
 */
@RestController
public class Demo {
    @GetMapping("/vol")
    public Map saleVol() throws IOException {
        Iterator<Result> iterator = HBaseUtil.scanTable("", "demo", "sale_vol");
        return build(iterator);
    }

    @GetMapping("/profit")
    public Map<String, String> profit() throws IOException {

        Iterator<Result> iterator = HBaseUtil.scanTable("", "demo", "profit");
        return build(iterator);
    }

    private Map<String, String> build(Iterator<Result> iterator) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        if (null != iterator) {
            while (iterator.hasNext()) {
                Result next = iterator.next();
                CellScanner cellScanner = next.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    map.put(new String(CellUtil.cloneQualifier(cell), "utf-8"), new String(CellUtil.cloneValue(cell), "utf-8"));
                }
            }
        }
        return map;
    }

}
