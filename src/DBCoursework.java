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
            //Thread.sleep(500);

            DBCoursework db = new DBCoursework(toConnect);

            db.declareTables();

            System.out.println("Reading CSV files now...");
            //Thread.sleep(500);


            // Read drivers
            db.driversReader.read();

            // Store headers and data
            String[] driverHeaders = db.driversReader.getHeaders();
            String[][] driverData = db.driversReader.getData();

            // Set headers and data to upload
            db.driversUploader.setHeaders(driverHeaders);
            db.driversUploader.setDataRows(driverData);

            System.out.println("Driver headers: " + String.join(", ", driverHeaders));
            //Thread.sleep(500);


            // Read teams
            db.driving_teamReader.read();

            // Store headers and data
            String[] teamHeaders = db.driving_teamReader.getHeaders();
            String[][] teamData = db.driving_teamReader.getData();

            // Set headers and data to upload
            db.teamUploader.setHeaders(teamHeaders);
            db.teamUploader.setDataRows(teamData);

            System.out.println("Racing Teams headers: " + String.join(", ", teamHeaders));
            //Thread.sleep(500);


            // Read engineers
            db.race_engineersReader.read();

            // Store headers and data
            String[] engineerHeaders = db.race_engineersReader.getHeaders();
            String[][] engineerData = db.race_engineersReader.getData();

            // Set headers and data to upload
            db.engineersUploader.setHeaders(engineerHeaders);
            db.engineersUploader.setDataRows(engineerData);

            System.out.println("Race Engineers headers: " + String.join(", ", engineerHeaders));
            ///Thread.sleep(500);


            // Read racetracks
            db.racetracksReader.read();

            // Store headers and data
            String[] trackHeaders = db.racetracksReader.getHeaders();
            String[][] trackData = db.racetracksReader.getData();

            // Set headers and data to upload
            db.tracksUploader.setHeaders(trackHeaders);
            db.tracksUploader.setDataRows(trackData);

            System.out.println("Racetrack headers: " + String.join(", ", trackHeaders));
            //Thread.sleep(500);


            System.out.println("All data has been set ready for upload, now uploading to DB...");
            //Thread.sleep(500);

            // Upload to DB
            db.driversUploader.upload();
            db.teamUploader.upload();
            db.engineersUploader.upload();
            db.tracksUploader.upload();

            System.out.println("All data has been uploaded to DB! \nProceding to queries...\n");


            // Queries
            // Query 1
            System.out.println("Query 1: Find the driver with the most wins in the 2023 season:\n");
            ResultSet rs = db.statement.executeQuery("SELECT driver_name, constructor, racer_number, points, podiums, wins, pole_positions, fastest_laps FROM drivers ORDER BY wins DESC");
            System.out.println("Drivers:");
            System.out.printf("%-20s | %-30s | %-12s | %-7s | %-8s | %-6s | %-16s | %-14s\n", "Driver Name", "Team", "Racer Number", "Points", "Podiums", "Wins", "Pole Positions", "Fastest Laps");
            System.out.printf("%-20s | %-30s | %-12s | %-7s | %-8s | %-6s | %-16s | %-14s\n", "-------------------", "------------------------------", "------------", "-------", "--------", "------", "----------------", "--------------");
            while (rs.next()) {
                System.out.printf("%-20s | %-30s | %-12s | %-7d | %-8d | %-6d | %-16d | %-14d\n",
                        rs.getString("driver_name"),
                        rs.getString("constructor"),
                        rs.getInt("racer_number"),
                        rs.getInt("points"),
                        rs.getInt("podiums"),
                        rs.getInt("wins"),
                        rs.getInt("pole_positions"),
                        rs.getInt("fastest_laps"));
            }

            // Query 2
            System.out.println("\nQuery 2: Find all the races that took place in the 2023 season ordered by date:\n");
            ResultSet rs2 = db.statement.executeQuery("SELECT round, grand_prix, country, continent, circuit, race_date FROM racetracks ORDER BY race_date ASC");
            System.out.println("Racetracks:");
            System.out.printf("%-5s | %-30s | %-25s | %-10s | %-50s | %-10s\n", "Round", "Grand Prix", "Country", "Continent", "Circuit", "Race Date");
            System.out.printf("%-5s | %-30s | %-25s | %-10s | %-50s | %-10s\n", "-----", "------------------------------", "-------------------------", "----------", "----------------------------------------", "----------");
            while (rs2.next()) {
                System.out.printf("%-5d | %-30s | %-25s | %-10s | %-50s | %-10s\n",
                        rs2.getInt("round"),
                        rs2.getString("grand_prix"),
                        rs2.getString("country"),
                        rs2.getString("continent"),
                        rs2.getString("circuit"),
                        rs2.getString("race_date"));
            }

            System.out.println("\nQuery 3: Getting all teams of the season and ordering by points:\n");
            ResultSet rs3 = db.statement.executeQuery("SELECT constructor, chassis, power_unit, team_principal, headquarters, year_of_entry, points, world_championships FROM driving_team ORDER BY points DESC");
            System.out.println("Teams:");
            System.out.printf("%-30s | %-10s | %-20s | %-40s | %-30s | %-15s | %-10s | %-20s\n", "Team Name", "Chassis", "Power Unit", "Team Principal", "Headquarters", "Year of Entry", "Points", "World Championships");
            System.out.printf("%-30s | %-10s | %-20s | %-40s | %-30s | %-15s | %-10s | %-20s\n", "------------------------------", "----------", "----------", "------------------", "------------------------------", "---------------", "----------", "-------------------");
            while (rs3.next()) {
                System.out.printf("%-30s | %-10s | %-20s | %-40s | %-30s | %-15s | %-10s | %-20s\n",
                        rs3.getString("constructor"),
                        rs3.getString("chassis"),
                        rs3.getString("power_unit"),
                        rs3.getString("team_principal"),
                        rs3.getString("headquarters"),
                        rs3.getString("year_of_entry"),
                        rs3.getInt("points"),
                        rs3.getInt("world_championships"));
            }

            // Query 4
            System.out.println("\nQuery 4: Get each driver along with its car and power unit:\n");
            ResultSet rs4 = db.statement.executeQuery("SELECT d.racer_number, d.driver_name, dt.constructor AS team_name, dt.chassis AS chassis, dt.power_unit AS power_unit, d.points, dt.points AS team_points FROM drivers d JOIN driving_team dt ON d.constructor = dt.constructor ORDER BY dt.points DESC");
            System.out.println("Driver-Team Relationship:");
            System.out.printf("%-12s | %-20s | %-30s | %-10s | %-20s | %-14s | %-14s |\n", "Racer Number", "Driver Name", "Team Name", "Chassis", "Power Unit", "Driver points", "Team Points");
            System.out.printf("%-12s | %-20s | %-30s | %-10s | %-20s | %-14s | %-14s |\n", "------------", "-----------", "---------", "-------", "----------", "-------------", "-----------");
            while (rs4.next()) {
                System.out.printf("%-12d | %-20s | %-30s | %-10s | %-20s | %-14s | %-14s |\n",
                        rs4.getInt("racer_number"),
                        rs4.getString("driver_name"),
                        rs4.getString("team_name"),
                        rs4.getString("chassis"),
                        rs4.getString("power_unit"),
                        rs4.getInt("points"),
                        rs4.getInt("team_points"));
            }

            // Query 5
            System.out.println("\nQuery 5: Get the top 5 drivers of the 2023 season:\n");
            ResultSet rs5 = db.statement.executeQuery("SELECT driver_name, constructor, racer_number, points, podiums, wins, pole_positions, fastest_laps FROM drivers ORDER BY points DESC LIMIT 5");
            System.out.println("Top 5 Drivers:");
            System.out.printf("%-20s | %-30s | %-12s | %-7s | %-8s | %-6s | %-16s | %-14s\n", "Driver Name", "Team", "Racer Number", "Points", "Podiums", "Wins", "Pole Positions", "Fastest Laps");
            System.out.printf("%-20s | %-30s | %-12s | %-7s | %-8s | %-6s | %-16s | %-14s\n", "-------------------", "------------------------------", "------------", "-------", "--------", "------", "----------------", "--------------");
            while (rs5.next()) {
                System.out.printf("%-20s | %-30s | %-12s | %-7d | %-8d | %-6d | %-16d | %-14d\n",
                        rs5.getString("driver_name"),
                        rs5.getString("constructor"),
                        rs5.getInt("racer_number"),
                        rs5.getInt("points"),
                        rs5.getInt("podiums"),
                        rs5.getInt("wins"),
                        rs5.getInt("pole_positions"),
                        rs5.getInt("fastest_laps"));
            }

            // Query 6
            System.out.println("\nQuery 6: Counting racetracks by continent:\n");
            ResultSet rs6 = db.statement.executeQuery("SELECT continent, COUNT(*) AS circuits FROM racetracks GROUP BY continent");
            System.out.println("Racetracks by Continent:");
            System.out.printf("%-10s | %-10s\n", "Continent", "Circuits");
            System.out.printf("%-10s | %-10s\n", "----------", "--------");
            while (rs6.next()) {
                System.out.printf("%-10s | %-10d\n",
                        rs6.getString("continent"),
                        rs6.getInt("circuits"));
            }

            // Query 7
            System.out.println("\nQuery 7: Get the top 5 fastest laps and driver from each racetrack:\n");
            ResultSet rs7 = db.statement.executeQuery("SELECT grand_prix, fastest_lap, fastest_lap_time FROM racetracks ORDER BY fastest_lap_time ASC");
            System.out.println("Fastest Laps:");
            System.out.printf("%-30s | %-20s | %-20s\n", "Grand Prix", "Fastest Lap", "Fastest Lap Time");
            System.out.printf("%-30s | %-20s | %-20s\n", "------------------------------", "------------------", "----------------");
            while (rs7.next()) {
                System.out.printf("%-30s | %-20s | %-20s\n",
                        rs7.getString("grand_prix"),
                        rs7.getString("fastest_lap"),
                        rs7.getString("fastest_lap_time"));
            }

            // Query 8
            System.out.println("\nQuery 8: Get average fastest lap time\n");
            ResultSet rs8 = db.statement.executeQuery("SELECT AVG((substr(fastest_lap_time, 1, instr(fastest_lap_time, ':') - 1) * 60 + substr(fastest_lap_time, instr(fastest_lap_time, ':') + 1, instr(fastest_lap_time, '.') - instr(fastest_lap_time, ':') - 1) + substr(fastest_lap_time, instr(fastest_lap_time, '.') + 1) / 1000.0)) AS average_lap_time_seconds FROM racetracks");
            System.out.println("Average Fastest Lap Time:");
            System.out.printf("%-10s\n", "Average Lap Time (MM:SS.sss)");
            System.out.printf("%-10s\n", "-------------------");
            while (rs8.next()) {
                double averageLapTimeSeconds = rs8.getDouble("average_lap_time_seconds");
                int minutes = (int) (averageLapTimeSeconds / 60);
                double fractionalSeconds = averageLapTimeSeconds % 60;
                int seconds = (int) fractionalSeconds;
                int milliseconds = (int) ((fractionalSeconds - seconds) * 1000);
                System.out.printf("%02d:%02d.%03d\n", minutes, seconds, milliseconds);
            }

            // Query 9
            System.out.println("\nQuery 9: Getting race engineers with their drivers by experience\n");
            ResultSet rs9 = db.statement.executeQuery("SELECT e.engineer_name, e.years_with_driver, e.years_of_f1_entry, d.driver_name, e.constructor, d.points FROM race_engineers e JOIN drivers d ON e.engineer_name = d.race_engineer ORDER BY e.years_of_f1_entry ASC");
            System.out.println("Race engineers by experience: ");
            System.out.printf("%-20s | %-24s | %-20s | %-20s | %-30s | %-10s\n", "Engineer Name", "Year Started With Driver", "Year of F1 Entry", "Driver managed", "Team", "Points");
            System.out.printf("%-20s | %-24s | %-20s | %-20s | %-30s | %-10s\n", "-------------------", "------------------------", "----------------", "--------------", "----", "------");
            while (rs9.next()) {
                System.out.printf("%-20s | %-24d | %-20d | %-20s | %-30s | %-10d\n",
                        rs9.getString("engineer_name"),
                        rs9.getInt("years_with_driver"),
                        rs9.getInt("years_of_f1_entry"),
                        rs9.getString("driver_name"),
                        rs9.getString("constructor"),
                        rs9.getInt("points"));
            }

            // Query 10
            System.out.println("\nQuery 10: Grouping drivers by countries of origin\n");
            ResultSet rs10 = db.statement.executeQuery("SELECT country_of_birth, COUNT(*) AS drivers FROM drivers GROUP BY country_of_birth UNION ALL SELECT 'Total', COUNT(*) FROM drivers");
            System.out.println("Drivers by Country of Origin:");
            System.out.printf("%-24s | %-10s\n", "Country", "Drivers");
            System.out.printf("%-24s | %-10s\n", "-------", "-------");
            while (rs10.next()) {
                String country = rs10.getString("country_of_birth");
                System.out.printf("%-24s | %-10d\n", country, rs10.getInt("drivers"));
            }

            // Query 11
            System.out.println("\nQuery 11: Getting drivers that entered before 2015 and have won at least one world championship\n");
            ResultSet rs11 = db.statement.executeQuery("SELECT racer_number, driver_name, year_of_entry, world_championships FROM drivers WHERE year_of_entry < 2015 AND world_championships > 0 ORDER BY world_championships DESC");
            System.out.println("Drivers that entered before 2015 and have won at least one world championship:");
            System.out.printf("%-15s | %-20s | %-15s | %-20s\n", "Driver Number", "Driver Name", "Year of Entry", "World Championships");
            System.out.printf("%-15s | %-20s | %-15s | %-20s\n", "-------------","-------------------", "--------------", "-------------------");
            while (rs11.next()) {
                System.out.printf("%-15s | %-20s | %-15s | %-20d\n",
                        rs11.getInt("racer_number"),
                        rs11.getString("driver_name"),
                        rs11.getString("year_of_entry"),
                        rs11.getInt("world_championships"));
            }

            // Query 12
            System.out.println("\nQuery 12: Getting teams, drivers and race engineers\n");
            ResultSet rs12 = db.statement.executeQuery("WITH TeamDriverEngineer AS (SELECT dt.constructor AS team_name, dt.team_principal, re.engineer_name, d.driver_name, ROW_NUMBER() OVER (PARTITION BY dt.constructor, d.driver_name ORDER BY d.driver_name) AS row_num FROM driving_team dt JOIN race_engineers re ON dt.constructor = re.constructor JOIN drivers d ON re.engineer_name = d.race_engineer) SELECT team_name, team_principal, engineer_name, driver_name FROM TeamDriverEngineer WHERE row_num <= 2 ORDER BY team_name, driver_name");
            System.out.println("Teams, Team Principals, Drivers and Race Engineers:");
            System.out.printf("%-30s | %-40s | %-20s | %-20s\n", "Team Name", "Team Principal", "Drivers", "Race Engineers");
            System.out.printf("%-30s | %-40s | %-20s | %-20s\n", "------------------------------", "----------------------", "-------", "--------------");
            while (rs12.next()) {
                System.out.printf("%-30s | %-40s | %-20s | %-20s\n",
                        rs12.getString("team_name"),
                        rs12.getString("team_principal"),
                        rs12.getString("driver_name"),
                        rs12.getString("engineer_name"));
            }

            System.out.println("Nyck de Vries was fired from Formula 1 mid-season, and replaced by Daniel Ricciardo. \nAlso, Liam Lawson is a reserve driver, DB means to keep integrity on the main drivers, so deleting both from database\n");
            db.statement.executeUpdate("DELETE FROM drivers WHERE driver_name = 'Nyck de Vries' OR driver_name = 'Liam Lawson'");

            System.out.println("Nick de Vries and Liam Lawson have been deleted from the database\n Updated table:");
            ResultSet rs13 = db.statement.executeQuery("WITH TeamDriverEngineer AS (SELECT dt.constructor AS team_name, dt.team_principal, re.engineer_name, d.driver_name, ROW_NUMBER() OVER (PARTITION BY dt.constructor, d.driver_name ORDER BY d.driver_name) AS row_num FROM driving_team dt JOIN race_engineers re ON dt.constructor = re.constructor JOIN drivers d ON re.engineer_name = d.race_engineer) SELECT team_name, team_principal, engineer_name, driver_name FROM TeamDriverEngineer WHERE row_num <= 2 ORDER BY team_name, driver_name");
            System.out.println("Teams, Team Principals, Drivers and Race Engineers:");
            System.out.printf("%-30s | %-40s | %-20s | %-20s\n", "Team Name", "Team Principal", "Drivers", "Race Engineers");
            System.out.printf("%-30s | %-40s | %-20s | %-20s\n", "------------------------------", "----------------------", "-------", "--------------");
            while (rs13.next()) {
                System.out.printf("%-30s | %-40s | %-20s | %-20s\n",
                        rs13.getString("team_name"),
                        rs13.getString("team_principal"),
                        rs13.getString("driver_name"),
                        rs13.getString("engineer_name"));
            }


            // Query 14
            System.out.println("\nQuery 13: The Emilia Romagna Grand Prix was cancelled due to floods in the region... Deleting from database\n");
            db.statement.executeUpdate("DELETE FROM racetracks WHERE grand_prix = 'Emilia Romagna Grand Prix'");
            System.out.println("Emilia Romagna Grand Prix has been deleted from the database\n Updated table:");
            ResultSet rs14 = db.statement.executeQuery("SELECT round, grand_prix, country, continent, circuit, laps, race_date FROM racetracks");
            System.out.println("Racetracks:");
            System.out.printf("%-5s | %-30s | %-25s | %-10s | %-50s | %-10s | %-12s\n", "Round", "Grand Prix", "Country", "Continent", "Circuit", "Laps Amt", "Date of Race");
            System.out.printf("%-5s | %-30s | %-25s | %-10s | %-50s | %-10s | %-12s\n", "-----", "------------------------------", "-------------------------", "----------", "----------------------------------------", "----------", "------------");
            while (rs14.next()) {
                System.out.printf("%-5d | %-30s | %-25s | %-10s | %-50s | %-10s | %-12s\n",
                        rs14.getInt("round"),
                        rs14.getString("grand_prix"),
                        rs14.getString("country"),
                        rs14.getString("continent"),
                        rs14.getString("circuit"),
                        rs14.getInt("laps"),
                        rs14.getString("race_date"));
            }

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
            //Thread.sleep(500);

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
            //Thread.sleep(500);

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
            //Thread.sleep(500);

            statement.executeUpdate("DROP TABLE IF EXISTS race_engineers");
            statement.executeUpdate("CREATE TABLE race_engineers" +
                    "(engineer_id INTEGER, " +
                    "engineer_name TEXT PRIMARY KEY, " +
                    "constructor TEXT, " +
                    "years_with_driver INTEGER, " +
                    "years_of_f1_entry INTEGER, " +
                    "FOREIGN KEY (constructor) REFERENCES driving_team(constructor))");

            System.out.println("Created race_engineers table\n");
            //Thread.sleep(500);


            statement.executeUpdate("DROP TABLE IF EXISTS racetracks");
            statement.executeUpdate("CREATE TABLE racetracks" +
                    "(round INTEGER, " +
                    "grand_prix TEXT PRIMARY KEY, " +
                    "country TEXT, " +
                    "continent TEXT, " +
                    "circuit TEXT, " +
                    "laps INTEGER, " +
                    "race_date TEXT, " +
                    "first_place TEXT, " +
                    "second_place TEXT, " +
                    "third_place TEXT, " +
                    "fastest_lap TEXT, " +
                    "fastest_lap_time TEXT, " +
                    "FOREIGN KEY (first_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (second_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (third_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (fastest_lap) REFERENCES drivers(driver_name))");

            statement.executeUpdate(
                    "CREATE TRIGGER update_rounds_after_delete " +
                            "AFTER DELETE ON racetracks " +
                            "BEGIN " +
                            "UPDATE racetracks SET round = round - 1 WHERE round > OLD.round; " +
                            "END;"
            );

            System.out.println("Created racetracks table \nNow returning to read CSV files...\n");
            //Thread.sleep(500);
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
                //Thread.sleep(500);

                headers = allRows.get(0);

                int numColumns = headers.length;
                data = new String[numColumns][];

                for (int i = 0; i < numColumns; i++) {
                    data[i] = new String[allRows.size()];
                }

                System.out.println("Strings initialized, now processing data");
                //Thread.sleep(500);


                for(int i = 1; i < allRows.size(); i++) {
                    String[] row = allRows.get(i);

                    for (int j = 0; j < numColumns; j++) {
                        data[j][i-1] = row[j];
                    }
                }

                System.out.println("Read " + file + " successfully!");
                System.out.println("Received " + headers.length + " columns and " + data[0].length + " rows. \nTable ready for upload!\n");
                //Thread.sleep(500);

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
                //Thread.sleep(100);

                int querycount = 0;

                for (int i = 0; i < dataRows[0].length-1; i++) {
                    querycount++;
                    String query = "INSERT INTO " + tableToUpload + " (";

                    for (int j = 0; j < headers.length; j++) {
                        query += headers[j];

                        if (j != headers.length - 1) {
                            query += ", ";
                        }
                    }
                    query += ") VALUES (";

                    System.out.println("Uploading row " + i + " to " + tableToUpload + "...");
                    Thread.sleep(50);

                    for (int j = 0; j < headers.length; j++) {
                        query += "'" + dataRows[j][i] + "'";

                        if (j != headers.length - 1) {
                            query += ", ";
                        }
                    }
                    query += ")";

                    statement.executeUpdate(query);
                }

                System.out.println("\nData uploaded to: " + tableToUpload + " successfully!, " + querycount + " queries sent.\n\n");
                //Thread.sleep(500);

            } catch (Exception e) {
                System.out.println("Error through process: " + e);
                Thread.sleep(5000);

            }
        }
    }
}