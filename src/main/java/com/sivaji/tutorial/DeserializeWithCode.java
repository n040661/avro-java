package com.sivaji.tutorial;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.sivaji.entity.Employee_Record;

public class DeserializeWithCode {
    public static void main(String[] args) throws IOException {
        File file = new File("employees.avro");
        DatumReader<Employee_Record> userDatumReader = new SpecificDatumReader<Employee_Record>(Employee_Record.class);
        DataFileReader<Employee_Record> dataFileReader = new DataFileReader<Employee_Record>(file, userDatumReader);
        Employee_Record emp = null;
        while (dataFileReader.hasNext())
        {
            emp = dataFileReader.next();
            System.out.println(emp);
        }
    }
}
