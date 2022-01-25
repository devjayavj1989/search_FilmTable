;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;
import java.util.Scanner;



public class search_filmTable {
    public static void main(String[] args) {
        try{
            Scanner sn = new Scanner(System.in);

            String searchFinder =sn.nextLine();
            String queryString=("select a.first_name as,a.last_name ,c.name as category_name,f.*\n" +
                    "from actor as a join film_actor as fa\n" +
                    "on a.actor_id=fa.actor_id \n" +
                    "join film as f \n" +
                    "on f.film_id=fa.film_id \n" +
                    "join film_category as fc\n" +
                    "on f.film_id=fc.film_id\n" +
                    "join category as c \n" +
                    "on fc.category_id=c.category_id where a.first_name like 'P%'  or a.last_name like 'P%'");
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        Class.forName("org.postgresql.Driver");
        // Retrieving data from database
        PCollection<String> data = p.apply(JdbcIO.<String>read()

                // Data Source Configuration for PostgreSQL
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration

                        // Data Source Configuration for PostgreSQL
                        .create("org.postgresql.Driver","jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/jayavj1989")

                        //Data Source Configuration for MySQL
//						.create("com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/your_db?useSSL=false")

                        .withUsername("dbmasteruser").withPassword("Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<"))

                .withQuery(queryString));
                //.withCoder(StringUtf8Coder.of())
               // .withRowMapper(new JdbcIO.RowMapper<String>() {
//                                   private static final long serialVersionUID = 1L;
//                                   public String mapRow(ResultSet resultSet) throws Exception {
//                                       return "ID: "+resultSet.getInt("film_id")+"\nFirstName: "+resultSet.getString("first_name")+
//                                               "\nLastName: "+resultSet.getString("last_name");
//
//                                   }
//                               }
            //    ));
        //PCollection<String> data_json=data.apply("JSon Transform", AsJsons.of(String.class));
       // System.out.println(data_json.expand().values().);
        //data.apply(TextIO.write().to("gs://jaya_final_capstone_test_bucket/postgresql.txt"));
        p.run().waitUntilFinish();
        }
        catch(Exception e){
            System.out.println(e);}
    }

}
