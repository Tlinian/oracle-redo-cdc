import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try (var connection = DriverManager.getConnection(
            "jdbc:oracle:thin:@localhost:1521:ORCLCDB",
            "system",
            "123456"
        )) {
            PreparedStatement statement = connection.prepareStatement("insert into C##TEST_REDO.TAB01 values(?, ?)");
            for (int i = 0; i < 3; i++) {
//                statement.setInt(1, i);
                statement.setInt(1, i);
                statement.setString(2, "test" + i);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
