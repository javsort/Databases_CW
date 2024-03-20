import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class DBCoursework {

    CSV_Reader driversReader;
    CSV_Reader driving_teamReader;
    CSV_Reader race_engineersReader;
    CSV_Reader racetracksReader;

    UploadToDB driversUploader;
    UploadToDB teamUploader;
    UploadToDB engineersUploader;
    UploadToDB tracksUploader;

    Connection conn;
    Statement statement;

    public static void main(String[] args) throws InterruptedException {
        Connection toConnect;

        try {
            toConnect = DriverManager.getConnection("jdbc:sqlite:formula_1.db");
            System.out.println("Connection established, sending to main class...");
            Thread.sleep(500);

            DBCoursework db = new DBCoursework(toConnect);

            db.declareTables();

            System.out.println("Reading CSV files now...");
            Thread.sleep(500);


            // Read drivers
            db.driversReader.read();

            // Store headers and data
            String[] driverHeaders = db.driversReader.getHeaders();
            String[][] driverData = db.driversReader.getData();

            // Set headers and data to upload
            db.driversUploader.setHeaders(driverHeaders);
            db.driversUploader.setDataRows(driverData);

            System.out.println("Driver headers: " + String.join(", ", driverHeaders));
            Thread.sleep(500);


            // Read teams
            db.driving_teamReader.read();

            // Store headers and data
            String[] teamHeaders = db.driving_teamReader.getHeaders();
            String[][] teamData = db.driving_teamReader.getData();

            // Set headers and data to upload
            db.teamUploader.setHeaders(teamHeaders);
            db.teamUploader.setDataRows(teamData);

            System.out.println("Racing Teams headers: " + String.join(", ", teamHeaders));
            Thread.sleep(500);


            // Read engineers
            db.race_engineersReader.read();

            // Store headers and data
            String[] engineerHeaders = db.race_engineersReader.getHeaders();
            String[][] engineerData = db.race_engineersReader.getData();

            // Set headers and data to upload
            db.engineersUploader.setHeaders(engineerHeaders);
            db.engineersUploader.setDataRows(engineerData);

            System.out.println("Race Engineers headers: " + String.join(", ", engineerHeaders));
            Thread.sleep(500);


            // Read racetracks
            db.racetracksReader.read();

            // Store headers and data
            String[] trackHeaders = db.racetracksReader.getHeaders();
            String[][] trackData = db.racetracksReader.getData();

            // Set headers and data to upload
            db.tracksUploader.setHeaders(trackHeaders);
            db.tracksUploader.setDataRows(trackData);

            System.out.println("Racetrack headers: " + String.join(", ", trackHeaders));
            Thread.sleep(500);


            System.out.println("All data has been set ready for upload, now uploading to DB...");
            Thread.sleep(500);

            // Upload to DB
            db.driversUploader.upload();
            db.teamUploader.upload();
            db.engineersUploader.upload();
            db.tracksUploader.upload();

            System.out.println("All data has been uploaded to DB! \nProceding to queries...\n");

            // Queries
            // Query 1
            System.out.println("Query 1: Find the driver with the most wins in the 2023 season");
            ResultSet rs = db.statement.executeQuery("SELECT driver_name, constructor, racer_number, points, podiums, wins, pole_positions, fastest_laps FROM drivers ORDER BY wins");
            while (rs.next()) {
                System.out.println("Driver: " + rs.getString("driver_name") + " | Team: " + rs.getString("constructor") + " | Racer Number: " + rs.getInt("racer_number") + " | Points: " + rs.getInt("points") + " | Podiums: " + rs.getInt("podiums") + " | Wins: " + rs.getInt("wins") + " | Pole Positions: " + rs.getInt("pole_positions") + " | Fastest Laps: " + rs.getInt("fastest_laps"));
            }

            // Query 2
            System.out.println("\nQuery 2: Find the driver with the most podiums in the 2023 season");
            ResultSet rs2 = db.statement.executeQuery("SELECT driver_name, constructor, racer_number, points, podiums, wins, pole_positions, fastest_laps FROM drivers ORDER BY podiums");
            while (rs2.next()) {
                System.out.println("Driver: " + rs2.getString("driver_name") + " | Team: " + rs2.getString("constructor") + " | Racer Number: " + rs2.getInt("racer_number") + " | Points: " + rs2.getInt("points") + " | Podiums: " + rs2.getInt("podiums") + " | Wins: " + rs2.getInt("wins") + " | Pole Positions: " + rs2.getInt("pole_positions") + " | Fastest Laps: " + rs2.getInt("fastest_laps"));
            }

            Thread.sleep(1000);

        } catch (Exception e) {
            System.out.println("Error through process: " + e);
            Thread.sleep(5000);
        }

    }

    public DBCoursework(Connection toConnect) throws InterruptedException {
        driversReader = new CSV_Reader("./csv_files/drivers.csv");
        driving_teamReader = new CSV_Reader("./csv_files/driving_team.csv");
        race_engineersReader = new CSV_Reader("./csv_files/race_engineers.csv");
        racetracksReader = new CSV_Reader("./csv_files/racetracks.csv");

        try {
            conn = toConnect;
            statement = conn.createStatement();
            statement.setQueryTimeout(30);
            System.out.println("Connection established! \nNow proceeding to declare tables...");
            Thread.sleep(500);

        } catch (Exception e) {
            System.out.println("Error through process: " + e);
            Thread.sleep(5000);
        }

        driversUploader = new UploadToDB("drivers");
        teamUploader = new UploadToDB("driving_team");
        engineersUploader = new UploadToDB("race_engineers");
        tracksUploader = new UploadToDB("racetracks");


    }

    public void declareTables() throws InterruptedException {
        // Connect to db, initiate Schema, create tables
        try {
            // Creating tables:
            statement.executeUpdate("DROP TABLE IF EXISTS drivers");
            statement.executeUpdate("CREATE TABLE drivers" +
                    "(driver_id INTEGER, " +
                    "driver_name TEXT PRIMARY KEY , " +
                    "constructor TEXT, " +
                    "racer_number INTEGER, " +
                    "points INTEGER, " +
                    "country_of_birth TEXT, " +
                    "podiums INTEGER, " +
                    "wins INTEGER, " +
                    "pole_positions INTEGER, " +
                    "fastest_laps INTEGER, " +
                    "laps_raced INTEGER, " +
                    "km_raced INTEGER, " +
                    "laps_led INTEGER, " +
                    "best_result INTEGER, " +
                    "best_grid_pos INTEGER, " +
                    "world_championships INTEGER, " +
                    "year_of_entry TEXT, " +
                    "race_engineer TEXT, " +
                    "FOREIGN KEY (constructor) REFERENCES driving_team(constructor), " +
                    "FOREIGN KEY (race_engineer) REFERENCES race_engineers(engineer_name))");

            System.out.println("Created drivers table\n");
            Thread.sleep(500);

            statement.executeUpdate("DROP TABLE IF EXISTS driving_team");
            statement.executeUpdate("CREATE TABLE driving_team" +
                    "(team_id INTEGER, " +
                    "entrant TEXT, " +
                    "constructor TEXT PRIMARY KEY, " +
                    "chassis TEXT, " +
                    "power_unit TEXT, " +
                    "team_principal TEXT, " +
                    "headquarters TEXT, " +
                    "year_of_entry INTEGER, " +
                    "points INTEGER, " +
                    "world_championships INTEGER)");

            System.out.println("Created driving_team table\n");
            Thread.sleep(500);

            statement.executeUpdate("DROP TABLE IF EXISTS race_engineers");
            statement.executeUpdate("CREATE TABLE race_engineers" +
                    "(engineer_id INTEGER, " +
                    "engineer_name TEXT PRIMARY KEY, " +
                    "constructor TEXT, " +
                    "years_with_driver INTEGER, " +
                    "years_of_f1_entry INTEGER, " +
                    "FOREIGN KEY (constructor) REFERENCES driving_team(constructor))");

            System.out.println("Created race_engineers table\n");
            Thread.sleep(500);


            statement.executeUpdate("DROP TABLE IF EXISTS racetracks");
            statement.executeUpdate("CREATE TABLE racetracks" +
                    "(round INTEGER, " +
                    "grand_prix TEXT PRIMARY KEY, " +
                    "country TEXT, " +
                    "circuit TEXT, " +
                    "laps INTEGER, " +
                    "race_date TEXT, " +
                    "first_place TEXT, " +
                    "second_place TEXT, " +
                    "third_place TEXT, " +
                    "fastest_lap TEXT, " +
                    "FOREIGN KEY (first_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (second_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (third_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (fastest_lap) REFERENCES drivers(driver_name))");

            System.out.println("Created racetracks table \nNow returning to read CSV files...\n");
            Thread.sleep(500);

            Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println("Error through process: " + e);
            Thread.sleep(5000);
        }
    }

    public class CSV_Reader {
        private String file;
        private String line;
        private List<String[]> allRows = new ArrayList<>();
        private String[] headers;
        private String[][] data;

        public CSV_Reader(String file) {
            this.file = file;
        }

        public void read() throws InterruptedException {
            System.out.println("Reading " + file + " now...");
            try {
                BufferedReader br = new BufferedReader(new FileReader(this.file));

                while ((line = br.readLine()) != null) {
                    if(line.trim().isEmpty()){
                        continue;
                    }

                    String[] row = line.split(",");
                    allRows.add(row);
                }

                System.out.println("Read file successfully, now processing...\n");
                Thread.sleep(500);

                headers = allRows.get(0);

                int numColumns = headers.length;
                data = new String[numColumns][];

                for (int i = 0; i < numColumns; i++) {
                    data[i] = new String[allRows.size()];
                }

                System.out.println("Strings initialized, now processing data");
                Thread.sleep(500);


                for(int i = 1; i < allRows.size(); i++) {
                    String[] row = allRows.get(i);

                    for (int j = 0; j < numColumns; j++) {
                        data[j][i-1] = row[j];
                    }
                }

                System.out.println("Read " + file + " successfully!");
                System.out.println("Received " + headers.length + " columns and " + data[0].length + " rows. \nTable ready for upload!\n");
                Thread.sleep(500);

                /*for(int i = 0; i < numColumns; i++) {
                    System.out.println("Attribute " + headers[i] + ": " + String.join(", ", data[i]));
                    Thread.sleep(500);
                }*/

            } catch (Exception e){
                System.out.println("Error through process: " + e);
                Thread.sleep(5000);
            }
        }

        public String[] getHeaders() {
            return headers;
        }

        public String[][] getData() {
            return data;
        }
    }

    public class UploadToDB {
        String tableToUpload;

        String headers[];

        String dataRows[][];

        public UploadToDB(String table) {
            this.tableToUpload = table;
        }

        public void setHeaders(String[] headers) {
            this.headers = headers;
        }

        public void setDataRows(String[][] dataRows) {
            this.dataRows = dataRows;
        }

        public void upload() throws InterruptedException {
            try {
                System.out.println("Uploading data to " + tableToUpload + " now...\n");
                Thread.sleep(100);

                for (int i = 0; i < dataRows[0].length-1; i++) {

                    String query = "INSERT INTO " + tableToUpload + " (";

                    for (int j = 0; j < headers.length; j++) {
                        query += headers[j];

                        if (j != headers.length - 1) {
                            query += ", ";
                        }
                    }
                    query += ") VALUES (";

                    System.out.println("Uploading row " + i + " to " + tableToUpload + "...");
                    Thread.sleep(500);

                    for (int j = 0; j < headers.length; j++) {
                        query += "'" + dataRows[j][i] + "'";

                        if (j != headers.length - 1) {
                            query += ", ";
                        }
                    }
                    query += ")";

                    System.out.println("Query: " + query);
                    Thread.sleep(50);

                    statement.executeUpdate(query);
                }

                System.out.println("\nData uploaded to: " + tableToUpload + " successfully!\n\n");
                Thread.sleep(1000);

            } catch (Exception e) {
                System.out.println("Error through process: " + e);
                Thread.sleep(5000);

            }
        }
    }
}