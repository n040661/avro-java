package com.sivaji.tutorial;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.sivaji.entity.Employee_Record;

public class GenerateDataWithCode {

    public static void main(String[] args) throws IOException {
        Employee_Record emp1 = new Employee_Record();
        emp1.setName("Ram");
        emp1.setJoiningDate("12-21-2009");
        emp1.setRole("Hadoop Developer");
        // Leave dept null
        emp1.setSalary((float) 25000.00);

        // Alternate constructor
        Employee_Record emp2 = new Employee_Record("Bharath",
                "10- 20-2005", "Team Lead", "AIM", (float) 75000.00);

        // Construct via builder
        Employee_Record emp3 =
                Employee_Record.newBuilder()
                        .setName("Charlie")
                        .setJoiningDate("07-10-2002")
                        .setRole("Project Manager")
                        .setDept(null)
                        .setSalary((float) 125000.00)
                        .build();

        // Serialize emp1 and emp2 to disk
        File file = new File("employees.avro");
        DatumWriter<Employee_Record> userDatumWriter = new
                SpecificDatumWriter<Employee_Record>(Employee_Record.class);
        DataFileWriter<Employee_Record> dataFileWriter = new
                DataFileWriter<Employee_Record>(userDatumWriter);

        dataFileWriter.create(emp1.getSchema(), file);

        dataFileWriter.append(emp1);
        dataFileWriter.append(emp2);
        dataFileWriter.append(emp3);
        dataFileWriter.close();
    }
}
