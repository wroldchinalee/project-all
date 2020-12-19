package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.fastjson.JSON;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/11/23
 * @description: Json2ColumnsExample
 **/
public class Json2ColumnsExample {
//

    public static void main(String[] args){
        Row[] students = new Row[20];

        for(int i=0;i<20;i++){

            students[i] = Row.of( new Object[]{JSON.toJSONString(new Student(String.valueOf(i),i))});

        }

        String[] colnames = new String[] {"col1"};
        MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(students), colnames);
        List<Row> result = inOp.link(new JsonValueBatchOp()
                .setSkipFailed(true)
                .setSelectedCol("col1").setOutputCols(new String[] {"name", "age"})
                .setJsonPath(new String[] {"$.age", "$.name"})).collect();

        for(Row r:result){
            System.out.println(r.toString());
        }

    }

    public static class Student{
        private String name;
        private Integer age;

        public Student(String name,Integer age){
            this.name = name;
            this.age = age;
        }


        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }
    }
}
