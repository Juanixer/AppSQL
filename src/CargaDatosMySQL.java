import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.Integer.parseInt;

public class CargaDatosMySQL {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl="jdbc:mysql://localhost:3306/prueba";
        String username="root";
        String password="lacasito69";

        String filePath="C:\\Users\\Tuvieja\\Downloads\\Crime_con0s.csv";



        Connection connection=null;


        try{
            connection= DriverManager.getConnection(jdbcUrl,username,password);
            connection.setAutoCommit(false);

            String sql="insert into data(dr_no,Date_rptd,Date_occ,time_occ,Area,AreaName," +
                    "Rpt_Dist_no,Part_1_2,Crm_cd,Crm_cd_desc,Monocodes,Vict_age,Vict_sex,Vict_descent," +
                    "Premis_cd,Premis_desc,Weapon_used_cd,Weapon_desc,Status,Status_desc,Crmd_cd1," +
                    "Crmd_cd2,Crmd_cd3,Crmd_cd4,Location) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement statement=connection.prepareStatement(sql);

            BufferedReader lineReader=new BufferedReader(new FileReader(filePath));

            String lineText;
            int count=0;
            int var = 1;
            lineReader.readLine();
            while ((lineText=lineReader.readLine())!=null){
                String[] data=lineText.split(";");

                String dr_no=data[0];
                String Date_rptd=data[1];
                String Date_occ=data[2];
                String time_occ=data[3];
                String Area=data[4];
                String AreaName=data[5];
                String Rpt_Dist_no=data[6];
                String Part_1_2=data[7];
                String Crm_cd=data[8];
                String Crm_cd_desc=data[9];
                String Monocodes=data[10];
                String Vict_age=data[11];
                String Vict_sex=data[12];
                String Vict_descent=data[13];
                String Premis_cd=data[14];
                String Premis_desc=data[15];
                String Weapon_used_cd=data[16];
                String Weapon_desc=data[17];
                String Status=data[18];
                String Status_desc=data[19];
                String Crmd_cd1=data[20];
                String Crmd_cd2=data[21];
                String Crmd_cd3=data[22];
                String Crmd_cd4=data[23];
                String Location=data[24];



                statement.setInt(1,parseInt(dr_no));
                statement.setString(2,Date_rptd);
                statement.setString(3,Date_occ);
                statement.setInt(4,parseInt(time_occ));
                statement.setInt(5,parseInt(Area));
                statement.setString(6,AreaName);
                statement.setInt(7,parseInt(Rpt_Dist_no));
                statement.setInt(8,parseInt(Part_1_2));
                statement.setInt(9,parseInt(Crm_cd));
                statement.setString(10,Crm_cd_desc);
                statement.setString(11,Monocodes);
                statement.setInt(12,parseInt(Vict_age));
                statement.setString(13,Vict_sex);
                statement.setString(14,Vict_descent);
                statement.setInt(15,parseInt(Premis_cd));
                statement.setString(16,Premis_desc);
                statement.setInt(17,parseInt(Weapon_used_cd));
                statement.setString(18,Weapon_desc);
                statement.setString(19,Status);
                statement.setString(20,Status_desc);
                statement.setInt(21,parseInt(Crmd_cd1));
                statement.setInt(22,parseInt(Crmd_cd2));
                statement.setInt(23,parseInt(Crmd_cd3));
                statement.setInt(24,parseInt(Crmd_cd4));
                statement.setString(25,Location);


                System.out.println(var);
                var++;

                statement.execute();
            }

            lineReader.close();
            connection.commit();
            connection.close();
            System.out.println("La carga de datos ha sido exitosa");

        }
        catch (Exception exception){
            connection.rollback();
            exception.printStackTrace();
        }

    }
}