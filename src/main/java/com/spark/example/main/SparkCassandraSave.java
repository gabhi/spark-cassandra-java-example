package com.spark.example.main;

//import static com.datastax.spark.connector.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class SparkCassandraSave implements Serializable {
	private transient SparkConf conf;

	private SparkCassandraSave(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		generateData(sc);
		sc.stop();
	}

	private void generateData(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		// Prepare the schema
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS java_api");
			session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE java_api.people (id INT,name TEXT,birth_date  TIMESTAMP,PRIMARY KEY (id))");
		}
		 
		savePeople(sc);
 
	}

	private void savePeople(JavaSparkContext sc) {
		 List<Person> people = Arrays.asList(
	                Person.newInstance(4, "John", new Date()),
	                Person.newInstance(5, "Anna", new Date()),
	                Person.newInstance(6, "Andrew", new Date())
	        );
	        JavaRDD<Person> rdd = sc.parallelize(people);
	        javaFunctions(rdd)
	                .writerBuilder("java_api", "people", mapToRow(Person.class))
	                .saveToCassandra();

	        
	        
//	        JavaPairRDD<Integer, Person> cassandraRowsRDD = javaFunctions(sc).cassandraTable("java_api", "people",
//					mapColumnTo(Person.class)).keyBy(new Function<Person, Integer>() {
//				@Override
//				public Integer call(Person person) throws Exception {
//					return person.id;
//				}
//			});
//	        
//	        List<Tuple2<Integer, Person>> l =  cassandraRowsRDD.collect();
//	        
//	        for (Iterator iterator = l.iterator(); iterator.hasNext();) {
//				String string = (String) iterator.next();
//				System.out.println(string);
//				
//			}
	        
	        
	        // use case: we want to read that data as an RDD of CassandraRows and convert them to strings...
//	        JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("java_api", "people")
//	                .map(new Function<CassandraRow, String>() {
//	                    @Override
//	                    public String call(CassandraRow cassandraRow) throws Exception {
//	                    	System.out.println(cassandraRow);
//	                        return cassandraRow.toString();
//	                    }
//	                });
//	        
//	        List<String> l = cassandraRowsRDD.collect();
//	        
//	        for (Iterator iterator = l.iterator(); iterator.hasNext();) {
//				String string = (String) iterator.next();
//				System.out.println(string);
//				
//			}
//	        System.out.println("Data as CassandraRows: \n" + StringUtils.join("\n", cassandraRowsRDD.collect()));		
	}

	public static void main(String[] args) {

		String master = "";
		String host = "";
		if (args.length != 2) {
			System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
			master = "local[4]";
			host = "localhost";
		} else {
			master = args[0];
			host = args[1];
		}

		SparkConf conf = new SparkConf();
		conf.setAppName("Java API demo");
		conf.setMaster(master);
		conf.set("spark.cassandra.connection.host", host);
		
		SparkCassandraSave app = new SparkCassandraSave(conf);
		app.run();
	}

	
	public static class Person implements Serializable {
        private Integer id;
        private String name;
        private Date birthDate;

        public static Person newInstance(Integer id, String name, Date birthDate) {
            Person person = new Person();
            person.setId(id);
            person.setName(name);
            person.setBirthDate(birthDate);
            return person;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getBirthDate() {
            return birthDate;
        }

        public void setBirthDate(Date birthDate) {
            this.birthDate = birthDate;
        }

        @Override
        public String toString() {
        	return id+ " " + name + " "+ birthDate;
        }
    }

}
