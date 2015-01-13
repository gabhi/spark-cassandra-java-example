package com.spark.example.main;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.*;

//import static com.datastax.spark.connector.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class App implements Serializable {
	private transient SparkConf conf;

	private App(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		generateData(sc);
		compute(sc);
		// showResults(sc);
		sc.stop();
	}

	private void generateData(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		// Prepare the schema
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS java_api");
			session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
			session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
			session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
		}

		// Prepare the products hierarchy
		List<Product> products = Arrays.asList(new Product(0, "All products", Collections.<Integer> emptyList()),
				new Product(1, "Product A", Arrays.asList(0)), new Product(4, "Product A1", Arrays.asList(0, 1)),
				new Product(5, "Product A2", Arrays.asList(0, 1)), new Product(2, "Product B", Arrays.asList(0)),
				new Product(6, "Product B1", Arrays.asList(0, 2)), new Product(7, "Product B2", Arrays.asList(0, 2)),
				new Product(3, "Product C", Arrays.asList(0)), new Product(8, "Product C1", Arrays.asList(0, 3)),
				new Product(9, "Product C2", Arrays.asList(0, 3)));

		JavaRDD<Product> productsRDD = sc.parallelize(products);
		javaFunctions(productsRDD).writerBuilder("java_api", "products", mapToRow(Product.class)).saveToCassandra();

		JavaRDD<Sale> salesRDD = productsRDD.filter(new Function<Product, Boolean>() {
			@Override
			public Boolean call(Product product) throws Exception {
				return product.getParents().size() == 2;
			}
		}).flatMap(new FlatMapFunction<Product, Sale>() {
			@Override
			public Iterable<Sale> call(Product product) throws Exception {
				Random random = new Random();
				List<Sale> sales = new ArrayList<>(1000);
				for (int i = 0; i < 1000; i++) {
					sales.add(new Sale(UUID.randomUUID(), product.getId(), BigDecimal.valueOf(random.nextDouble())));
				}
				return sales;
			}
		});

		javaFunctions(salesRDD).writerBuilder("java_api", "sales", mapToRow(Sale.class)).saveToCassandra();

	}

	private void compute(JavaSparkContext sc) {
		
		
		
		JavaPairRDD<Integer, Product> productsRDD = javaFunctions(sc).cassandraTable("java_api", "products",
				mapColumnTo(Product.class)).keyBy(new Function<Product, Integer>() {
			@Override
			public Integer call(Product product) throws Exception {
				return product.getId();
			}
		});

		JavaPairRDD<Integer, Sale> salesRDD = javaFunctions(sc).cassandraTable("java_api", "sales",
				mapColumnTo(Sale.class)).keyBy(new Function<Sale, Integer>() {
			@Override
			public Integer call(Sale sale) throws Exception {
				return sale.getProduct();
			}
		});

		JavaPairRDD<Integer, Tuple2<Sale, Product>> joinedRDD = salesRDD.join(productsRDD);

		JavaPairRDD<Integer, BigDecimal> allSalesRDD = joinedRDD
				.flatMap(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Sale, Product>>, Integer, BigDecimal>() {
					@Override
					public Iterable<Tuple2<Integer, BigDecimal>> call(Tuple2<Integer, Tuple2<Sale, Product>> input)
							throws Exception {
						Tuple2<Sale, Product> saleWithProduct = input._2();
						List<Tuple2<Integer, BigDecimal>> allSales = new ArrayList<>(saleWithProduct._2().getParents()
								.size() + 1);
						allSales.add(new Tuple2<>(saleWithProduct._1().getProduct(), saleWithProduct._1().getPrice()));
						for (Integer parentProduct : saleWithProduct._2().getParents()) {
							allSales.add(new Tuple2<>(parentProduct, saleWithProduct._1().getPrice()));
						}
						return allSales;
					}
				});

		JavaRDD<Summary> summariesRDD = allSalesRDD.reduceByKey(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
			@Override
			public BigDecimal call(BigDecimal v1, BigDecimal v2) throws Exception {
				return v1.add(v2);
			}
		}).map(new Function<Tuple2<Integer, BigDecimal>, Summary>() {
			@Override
			public Summary call(Tuple2<Integer, BigDecimal> input) throws Exception {
				return new Summary(input._1(), input._2());
			}
		});
		javaFunctions(summariesRDD).writerBuilder("java_api", "summaries", mapToRow(Summary.class)).saveToCassandra();

	}

	private void showResults(JavaSparkContext sc) {
		JavaPairRDD<Integer, Summary> summariesRdd = javaFunctions(sc).cassandraTable("java_api", "summaries",
				mapColumnTo(Summary.class)).keyBy(new Function<Summary, Integer>() {
			@Override
			public Integer call(Summary summary) throws Exception {
				return summary.getProduct();
			}
		});

		JavaPairRDD<Integer, Product> productsRdd = javaFunctions(sc).cassandraTable("java_api", "products",
				mapColumnTo(Product.class)).keyBy(new Function<Product, Integer>() {
			@Override
			public Integer call(Product product) throws Exception {
				return product.getId();
			}
		});

		List<Tuple2<Product, Optional<Summary>>> results = productsRdd.leftOuterJoin(summariesRdd).values().toArray();

		for (Tuple2<Product, Optional<Summary>> result : results) {
			System.out.println(result);
		}
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

		App app = new App(conf);
		app.run();
	}

	public static class Product implements Serializable {
		private Integer id;
		private String name;
		private List<Integer> parents;

		public Product() {
		}

		public Product(Integer id, String name, List<Integer> parents) {
			this.id = id;
			this.name = name;
			this.parents = parents;
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

		public List<Integer> getParents() {
			return parents;
		}

		public void setParents(List<Integer> parents) {
			this.parents = parents;
		}

		@Override
		public String toString() {
			return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name, parents);
		}
	}

	public static class Sale implements Serializable {
		private UUID id;
		private Integer product;
		private BigDecimal price;

		public Sale() {
		}

		public Sale(UUID id, Integer product, BigDecimal price) {
			this.id = id;
			this.product = product;
			this.price = price;
		}

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
		}

		public Integer getProduct() {
			return product;
		}

		public void setProduct(Integer product) {
			this.product = product;
		}

		public BigDecimal getPrice() {
			return price;
		}

		public void setPrice(BigDecimal price) {
			this.price = price;
		}

		@Override
		public String toString() {
			return MessageFormat.format("Sale'{'id={0}, product={1}, price={2}'}'", id, product, price);
		}
	}

	public static class Summary implements Serializable {
		private Integer product;
		private BigDecimal summary;

		public Summary() {
		}

		public Summary(Integer product, BigDecimal summary) {
			this.product = product;
			this.summary = summary;
		}

		public Integer getProduct() {
			return product;
		}

		public void setProduct(Integer product) {
			this.product = product;
		}

		public BigDecimal getSummary() {
			return summary;
		}

		public void setSummary(BigDecimal summary) {
			this.summary = summary;
		}

		@Override
		public String toString() {
			return MessageFormat.format("Summary'{'product={0}, summary={1}'}'", product, summary);
		}
	}

}