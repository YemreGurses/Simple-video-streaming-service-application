import java.io.File;
import java.sql.*;
import java.util.Date;
import java.util.Properties;
import java.io.FileInputStream;

public class Query {

	// DB Connections.
	private Connection _imdb;
	private Connection _customer_db;

	// Queries.
	private String _there_exists_such_plan_sql = "SELECT * FROM Plan WHERE plan_id = ?";
	private PreparedStatement _there_exists_such_plan_statement;

	private String _there_exists_such_movie_sql = "SELECT * FROM Movies WHERE tconst = ?";
	private PreparedStatement _there_exists_such_movie_statement;

    private String _there_exists_such_customer_sql = "SELECT * FROM Customer WHERE email = ?";
    private PreparedStatement _there_exists_such_customer_statement;

    private String sign_up_sql = "INSERT INTO Customer VALUES(DEFAULT, ?, ?, ?, ?, 0)";
    private PreparedStatement sign_up_statement;

    private String show_userid_sql = "SELECT customer_id FROM Customer WHERE email = ?";
    private PreparedStatement show_userid_statement;

    private String sign_up_plan_sql = "INSERT INTO Subscription VALUES(DEFAULT, ?, ?)";
    private PreparedStatement sign_up_plan_statement;

    private String sign_in_sql = "SELECT * FROM Customer WHERE email = ? AND password = ?";
    private PreparedStatement sign_in_statement;

    private String update_session_sql = "UPDATE Customer SET session_count = ? WHERE customer_id = ?";
    private PreparedStatement update_session_statement;

    private String sign_out_sql = "SELECT * FROM Customer WHERE customer_id = ?";
    private PreparedStatement sign_out_statement;

    private String show_plans_sql = "SELECT * FROM Plan";
    private PreparedStatement show_plans_statement;

    private String show_subscription_sql = "SELECT p.plan_id, p.name, p.resolution, p.max_parallel_session, p.monthly_fee " +
            "FROM Customer c, Subscription s, Plan p " +
            "WHERE c.customer_id=? and c.customer_id=s.customer_id and s.plan_id=p.plan_id";
    private PreparedStatement show_subscription_statement;

    private String get_max_parallel_sql = "SELECT p.max_parallel_session FROM Plan p WHERE p.plan_id = ?";
    private PreparedStatement get_max_parallel_statement;

    private String update_sub_sql = "UPDATE Subscription SET plan_id = ? WHERE customer_id = ?";
    private PreparedStatement update_sub_statement;

    private String watched_sql = "INSERT INTO Watched VALUES(DEFAULT, ?, ?, ?)";
    private PreparedStatement watched_statement;

    private String find_movie_sql = "SELECT m.tconst, m.originalTitle FROM Movies m WHERE m.originalTitle = ?";
    private PreparedStatement find_movie_statement;

    private String find_directors_sql = "SELECT d2.primaryName FROM Directed d, Directors d2 WHERE d.director = d2.nconst AND d.tconst = ?";
    private PreparedStatement find_directors_statement;

    private String find_actors_sql = "SELECT a.primaryName FROM Casts c, Actors a WHERE c.nconst = a.nconst AND c.tconst = ?";
    private PreparedStatement find_actors_statement;

    private String watched_or_not_sql = "SELECT w.movie_id FROM Customer c, Watched w WHERE c.customer_id = w.customer_id AND w.movie_id = ?";
    private PreparedStatement watched_or_not_statement;

    private String most_recently_watched_sql = "SELECT w.movie_id, w.time FROM Customer c, Watched w WHERE c.customer_id = w.customer_id AND c.customer_id = ? ORDER BY time DESC";
    private PreparedStatement most_recently_watched_statement;

    private String ascended_genre_sql = "SELECT g.genre FROM Movies m, Genres g WHERE m.tconst = g.tconst AND m.tconst = ? ORDER BY g.genre ASC";
    private PreparedStatement ascended_genre_statement;

    private String find_5_movies_sql = "SELECT * FROM Movies m, Genres g WHERE m.tconst = g.tconst AND g.genre = ? AND m.startYear = 2019 ORDER BY averageRating DESC LIMIT 5";
    private PreparedStatement find_5_movies_statement;

	public Query() {

	}

	public void openConnection() throws Exception {
		// DB connection configurations
		Properties configProps = new Properties();
		configProps.load(new FileInputStream("src/main/java/dbconn.config"));
		String mysqlDriver			= configProps.getProperty("mysqlDriver");
		String mysqlUrl				= configProps.getProperty("mysqlUrl");
		String mysqlUser			= configProps.getProperty("mysqlUser");
		String mysqlPassword		= configProps.getProperty("mysqlPassword");

		String postgresqlDriver		= configProps.getProperty("postgresqlDriver");
		String postgresqlUrl		= configProps.getProperty("postgresqlUrl");
		String postgresqlUser		= configProps.getProperty("postgresqlUser");
		String postgresqlPassword	= configProps.getProperty("postgresqlPassword");

		// Load jdbc drivers
		Class.forName(mysqlDriver);
		Class.forName(postgresqlDriver);

		// Open connections to two databases: imdb and the customer database
		_imdb = DriverManager.getConnection(mysqlUrl, // database
				mysqlUser, // user
				mysqlPassword); // password

            _customer_db = DriverManager.getConnection(postgresqlUrl, // database
                    postgresqlUser, // user
                    postgresqlPassword); // password

	}

	public void closeConnection() throws Exception {
		_imdb.close();
		_customer_db.close();
	}

  // Prepare all the SQL statements in this method.
	// Preparing a statement is almost like compiling it.
	// Note that the parameters (with ?) are still not filled in.

	public void prepareStatements() throws Exception {

		_there_exists_such_plan_statement = _customer_db.prepareStatement(_there_exists_such_plan_sql);
		_there_exists_such_movie_statement = _imdb.prepareStatement(_there_exists_such_movie_sql);
        _there_exists_such_customer_statement = _customer_db.prepareStatement(_there_exists_such_customer_sql);
        sign_up_statement = _customer_db.prepareStatement(sign_up_sql);
        show_userid_statement = _customer_db.prepareStatement(show_userid_sql);
        sign_up_plan_statement = _customer_db.prepareStatement(sign_up_plan_sql);
        sign_in_statement = _customer_db.prepareStatement(sign_in_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        sign_out_statement = _customer_db.prepareStatement(sign_out_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        show_plans_statement = _customer_db.prepareStatement(show_plans_sql);
        show_subscription_statement = _customer_db.prepareStatement(show_subscription_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        get_max_parallel_statement = _customer_db.prepareStatement(get_max_parallel_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        update_sub_statement = _customer_db.prepareStatement(update_sub_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        watched_statement = _customer_db.prepareStatement(watched_sql);
        update_session_statement = _customer_db.prepareStatement(update_session_sql);
        find_movie_statement = _imdb.prepareStatement(find_movie_sql);
        find_directors_statement = _imdb.prepareStatement(find_directors_sql);
        find_actors_statement = _imdb.prepareStatement(find_actors_sql);
        watched_or_not_statement = _customer_db.prepareStatement(watched_or_not_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        most_recently_watched_statement = _customer_db.prepareStatement(most_recently_watched_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ascended_genre_statement = _imdb.prepareStatement(ascended_genre_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        find_5_movies_statement = _imdb.prepareStatement(find_5_movies_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		// add here more prepare statements for all the other queries you need. TODO.
	}

	// You can add more methods if you need. TODO.

	public boolean helper_there_exists_such_plan(int plan_id) throws Exception {
		_there_exists_such_plan_statement.clearParameters();
		_there_exists_such_plan_statement.setInt(1, plan_id);

		ResultSet rs = _there_exists_such_plan_statement.executeQuery();

		boolean there_exists_such_plan = rs.next();

		rs.close();
		return there_exists_such_plan;
	}

	public boolean helper_there_exists_such_movie(String movie_id) throws Exception {
		_there_exists_such_movie_statement.clearParameters();
		_there_exists_such_movie_statement.setString(1, movie_id);

		ResultSet rs = _there_exists_such_movie_statement.executeQuery();

		boolean there_exists_such_movie = rs.next();

		rs.close();
		return there_exists_such_movie;
	}

	public boolean helper_already_signed_up(String email) throws Exception {
        _there_exists_such_customer_statement.clearParameters();
        _there_exists_such_customer_statement.setString(1, email);

        ResultSet rs = _there_exists_such_customer_statement.executeQuery();

        boolean there_exists_such_customer = rs.next();

        rs.close();
        return there_exists_such_customer;
	}

	public boolean transaction_sign_up(String email, String password, String first_name, String last_name, int plan_id) throws Exception {
        sign_up_statement.clearParameters();
        sign_up_statement.setString(1, email);
        sign_up_statement.setString(2, password);
        sign_up_statement.setString(3, first_name);
        sign_up_statement.setString(4, last_name);


        Integer rs = sign_up_statement.executeUpdate();
        if(rs==0){
            return false;
        }
        else{
            show_userid_statement.setString(1, email);
            ResultSet rs_id = show_userid_statement.executeQuery();
            rs_id.next();
            Integer user_id = rs_id.getInt("customer_id");

            sign_up_plan_statement.setInt(1, user_id);
            sign_up_plan_statement.setInt(2, plan_id);
            Integer rs2 = sign_up_plan_statement.executeUpdate();
            if(rs2==0){
                return false;
            }
            else {
                return true;
            }
        }
	}

	public Customer transaction_sign_in(String email, String password) throws Exception {
        sign_in_statement.clearParameters();
        sign_in_statement.setString(1, email);
        sign_in_statement.setString(2, password);


        _customer_db.setAutoCommit(false);

        ResultSet rs = sign_in_statement.executeQuery();

        if(rs.first()){
            Integer user_id = rs.getInt("customer_id");
            String user_email = rs.getString("email");
            String firstName = rs.getString("first_name");
            String lastName = rs.getString("last_name");
            Integer session_count = rs.getInt("session_count");

            show_subscription_statement.clearParameters();
            show_subscription_statement.setInt(1, user_id);

            ResultSet rs2 = show_subscription_statement.executeQuery();
            rs2.first();
            Integer max_session_count = rs2.getInt("max_parallel_session");

            if(session_count < max_session_count) {

                update_session_statement.clearParameters();
                update_session_statement.setInt(1, session_count+1);
                update_session_statement.setInt(2, user_id);
                update_session_statement.executeUpdate();
                _customer_db.commit();

                Customer c = new Customer();
                c.setCustomerId(user_id);
                c.setEmail(user_email);
                c.setFirstName(firstName);
                c.setLastName(lastName);

                return c;
            }
            else{
                _customer_db.rollback();
                System.out.println("You can not have more than " + max_session_count + " session(s) at the same time!");
                return null;
            }
        }
        else{

            return null;
        }

	}

	public boolean transaction_sign_out(int customer_id) throws Exception {
		sign_out_statement.clearParameters();
		sign_out_statement.setInt(1, customer_id);

		ResultSet rs = sign_out_statement.executeQuery();

		if(rs.first()){
		    Integer session_count = rs.getInt("session_count");

            update_session_statement.clearParameters();
            update_session_statement.setInt(1,session_count-1);
            update_session_statement.setInt(2, customer_id);
            update_session_statement.executeUpdate();
            _customer_db.commit();
		    return true;
        }
		else{
		    return false;
        }

	}

	public void transaction_show_plans() throws Exception {
		show_plans_statement.clearParameters();

		ResultSet rs = show_plans_statement.executeQuery();

        System.out.format("%-16s%-16s%-16s%-16s%-16s\n", "plan_id", "name", "resolution", "max_parallel", "monthly_fee");
		while(rs.next()){
		    Integer id = rs.getInt("plan_id");
		    String name = rs.getString("name");
            String resolution = rs.getString("resolution");
            Integer max_parallel = rs.getInt("max_parallel_session");
            Float monthly_fee = rs.getFloat("monthly_fee");
            System.out.format("%-16d%-16s%-16s%-16d%-16.2f\n", id, name, resolution, max_parallel, monthly_fee);
        }
	}

	public void transaction_show_subscription(int customer_id) throws Exception {
        show_subscription_statement.clearParameters();
        show_subscription_statement.setInt(1, customer_id);

        ResultSet rs = show_subscription_statement.executeQuery();

        System.out.format("%-16s%-16s%-16s%-16s%-16s\n", "plan_id", "name", "resolution", "max_parallel", "monthly_fee");
        while(rs.next()){
            Integer id = rs.getInt("plan_id");
            String name = rs.getString("name");
            String resolution = rs.getString("resolution");
            Integer max_parallel = rs.getInt("max_parallel_session");
            Float monthly_fee = rs.getFloat("monthly_fee");
            System.out.format("%-16d%-16s%-16s%-16d%-16.2f\n", id, name, resolution, max_parallel, monthly_fee);
        }
	}

	public boolean transaction_subscribe(int customer_id, int plan_id) throws Exception {
        _customer_db.setAutoCommit(false);

        show_subscription_statement.clearParameters();
        show_subscription_statement.setInt(1, customer_id);
        ResultSet rs = show_subscription_statement.executeQuery();
        rs.first();
        Integer current_session =  rs.getInt("max_parallel_session");

        get_max_parallel_statement.clearParameters();
        get_max_parallel_statement.setInt(1, plan_id);
        ResultSet rs2 = get_max_parallel_statement.executeQuery();
        rs2.first();
        Integer wanted_session = rs2.getInt("max_parallel_session");

        if(wanted_session < current_session){
            _customer_db.rollback();
            return false;
        }
        else{
            update_sub_statement.clearParameters();
            update_sub_statement.setInt(1, plan_id);
            update_sub_statement.setInt(2, customer_id);
            update_sub_statement.executeUpdate();
            _customer_db.commit();
            return true;
        }
	}

	public boolean transaction_watch(String movie_id, int customer_id) throws Exception {
        watched_statement.clearParameters();
        watched_statement.setString(1, movie_id);
        watched_statement.setInt(2, customer_id);
        Date uDate = new Date();
        java.sql.Date d = new java.sql.Date(uDate.getTime());
        watched_statement.setDate(3, d);

        Integer rs = watched_statement.executeUpdate();
        if(rs==0){
            return false;
        }
        else{
            _customer_db.commit();
            return true;
        }
	}

	public void transaction_search_for_movies(int customer_id, String movie_title) throws Exception {
		find_movie_statement.clearParameters();
		find_movie_statement.setString(1, movie_title);

		ResultSet rs_movies = find_movie_statement.executeQuery();

		while(rs_movies.next()){
		    String movie_id =  rs_movies.getString("tconst");
//            String movie_title2 =  rs_movies.getString("originalTitle");

            System.out.println("Movie ID: " + movie_id + "\t" + "Movie Title: " + movie_title);

            watched_statement.clearParameters();
            watched_or_not_statement.setString(1, movie_id);
            ResultSet watched_or_not = watched_or_not_statement.executeQuery();
            if(watched_or_not.first()){
                System.out.println("You have already watched " + movie_title + " movie! Why dont you try a new one");
            }
            else{
                System.out.println("Looks like " + movie_title + " movie is a new one for you!");
            }

		    find_directors_statement.clearParameters();
		    find_directors_statement.setString(1, movie_id);
		    ResultSet rs_directors = find_directors_statement.executeQuery();

		    System.out.println("Director(s):");
            while(rs_directors.next()){
                String dir_name = rs_directors.getString("primaryName");
                System.out.println(dir_name);
            }

		    find_actors_statement.clearParameters();
		    find_actors_statement.setString(1, movie_id);
		    ResultSet rs_actors = find_actors_statement.executeQuery();

            System.out.println("Actor(s):");
		    while(rs_actors.next()){
                String actor_name = rs_actors.getString("primaryName");
                System.out.println(actor_name);
            }
        }
	}

	public void transaction_suggest_movies(int customer_id) throws Exception {
        most_recently_watched_statement.clearParameters();
        most_recently_watched_statement.setInt(1, customer_id);
        ResultSet last_watched_movies = most_recently_watched_statement.executeQuery();
        if(last_watched_movies.first()) {
            String movie_id = last_watched_movies.getString("movie_id");

            ascended_genre_statement.clearParameters();
            ascended_genre_statement.setString(1, movie_id);
            ResultSet genres = ascended_genre_statement.executeQuery();
            genres.first();
            String genre = genres.getString("genre");

            find_5_movies_statement.clearParameters();
            find_5_movies_statement.setString(1, genre);
            ResultSet suggested_movies = find_5_movies_statement.executeQuery();

            System.out.format("%-16s%-32s%-16s\n", "Movie_id", "Title", "Average Rating");
            while (suggested_movies.next()) {
                String suggested_movie_id = suggested_movies.getString("tconst");
                String suggested_movie_title = suggested_movies.getString("originalTitle");
                Float suggested_rating = suggested_movies.getFloat("averageRating");

                watched_statement.clearParameters();
                watched_or_not_statement.setString(1, suggested_movie_id);
                ResultSet watched_or_not = watched_or_not_statement.executeQuery();
                if(!watched_or_not.first()){
                    System.out.format("%-16s%-32s%-16.2f\n", suggested_movie_id, suggested_movie_title, suggested_rating);
                }
            }
        }
	}

}
