import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnectionDriver {

	public static Connection connection;
	
	public static void connect() {
		try {
			connection = DriverManager.getConnection("jdbc:sqlite:purchases.db");
			System.out.println("Connected");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
