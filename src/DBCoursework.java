import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class DBCoursework {

    CSV_Reader driversReader;
    CSV_Reader driving_teamReader;
    CSV_Reader race_engineersReader;
    CSV_Reader racetracksReader;
    CSV_Reader drivingstatsReader;
    CSV_Reader racetrackstatsReader;


    UploadToDB driversUploader;
    UploadToDB teamUploader;
    UploadToDB engineersUploader;
    UploadToDB tracksUploader;
    UploadToDB drivingstatsUploader;
    UploadToDB racetrackstatsUploader;

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

            // Read drivers stats
            db.drivingstatsReader.read();

            // Store headers and data
            String[] driverStatsHeaders = db.drivingstatsReader.getHeaders();
            String[][] driverStatsData = db.drivingstatsReader.getData();

            // Set headers and data to upload
            db.drivingstatsUploader.setHeaders(driverStatsHeaders);
            db.drivingstatsUploader.setDataRows(driverStatsData);

            System.out.println("Driver stats headers: " + String.join(", ", driverStatsHeaders));
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


            // Read racetrack stats
            db.racetrackstatsReader.read();

            // Store headers and data
            String[] trackStatsHeaders = db.racetrackstatsReader.getHeaders();
            String[][] trackStatsData = db.racetrackstatsReader.getData();

            // Set headers and data to upload
            db.racetrackstatsUploader.setHeaders(trackStatsHeaders);
            db.racetrackstatsUploader.setDataRows(trackStatsData);

            System.out.println("Racetrack stats headers: " + String.join(", ", trackStatsHeaders));
            Thread.sleep(500);


            System.out.println("All data has been set ready for upload, now uploading to DB...");
            Thread.sleep(500);

            // Upload to DB
            db.driversUploader.upload();
            db.drivingstatsUploader.upload();
            db.teamUploader.upload();
            db.engineersUploader.upload();
            db.tracksUploader.upload();
            db.racetrackstatsUploader.upload();

            System.out.println("All data has been uploaded to DB! \nProceding to queries...\n");
            Thread.sleep(500);

            db.performQueries();

            System.out.println("\nQueries performed successfully! \nNow closing connection...\n");
            Thread.sleep(500);

            toConnect.close();

            System.out.println("Connection closed successfully! \nProgram finished. Bye.\n");

        } catch (Exception e) {
            System.out.println("Error through process: " + e);
            Thread.sleep(5000);
        }

    }

    public DBCoursework(Connection toConnect) throws InterruptedException {

        driversReader = new CSV_Reader("./csv_files/drivers_2.csv");
        driving_teamReader = new CSV_Reader("./csv_files/driving_team.csv");
        race_engineersReader = new CSV_Reader("./csv_files/race_engineers.csv");
        racetracksReader = new CSV_Reader("./csv_files/racetracks_2.csv");
        drivingstatsReader = new CSV_Reader("./csv_files/drivers_stats.csv");
        racetrackstatsReader = new CSV_Reader("./csv_files/racetracks_stats.csv");

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
        drivingstatsUploader = new UploadToDB("drivers_stats");
        racetrackstatsUploader = new UploadToDB("racetracks_stats");

    }

    public void declareTables() throws InterruptedException {
        // Connect to db, initiate Schema, create tables
        try {
            // Creating tables:
            statement.executeUpdate("DROP TABLE IF EXISTS drivers");

            statement.executeUpdate("CREATE TABLE drivers" +
                    "(driver_name TEXT PRIMARY KEY, " +
                    "constructor TEXT, " +
                    "racer_number INTEGER, " +
                    "country_of_birth TEXT, " +
                    "race_engineer TEXT, " +
                    "year_of_entry TEXT, " +
                    "FOREIGN KEY (constructor) REFERENCES driving_team(constructor), " +
                    "FOREIGN KEY (race_engineer) REFERENCES race_engineers(engineer_name))");

            System.out.println("Created drivers table\n");
            Thread.sleep(500);

            statement.executeUpdate("DROP TABLE IF EXISTS drivers_stats");
            statement.executeUpdate("CREATE TABLE drivers_stats" +
                    "(driver_name TEXT PRIMARY KEY, " +
                    "points INTEGER, " +
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
                    "FOREIGN KEY (driver_name) REFERENCES drivers(driver_name) ON DELETE CASCADE)");

            System.out.println("Created drivers_stats table\n");
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
                    "continent TEXT, " +
                    "circuit TEXT, " +
                    "laps INTEGER, " +
                    "race_date TEXT)");

            statement.executeUpdate(
                    "CREATE TRIGGER update_rounds_after_delete " +
                            "AFTER DELETE ON racetracks " +
                            "BEGIN " +
                            "UPDATE racetracks SET round = round - 1 WHERE round > OLD.round; " +
                            "END;"
            );

            System.out.println("Created racetracks table \n");
            Thread.sleep(500);

            statement.executeUpdate("DROP TABLE IF EXISTS racetracks_stats");
            statement.executeUpdate("CREATE TABLE racetracks_stats" +
                    "(grand_prix TEXT PRIMARY KEY, " +
                    "first_place TEXT, " +
                    "second_place TEXT, " +
                    "third_place TEXT, " +
                    "fastest_lap TEXT, " +
                    "fastest_lap_time TEXT, " +
                    "FOREIGN KEY (grand_prix) REFERENCES racetracks(grand_prix) ON DELETE CASCADE, " +
                    "FOREIGN KEY (first_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (second_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (third_place) REFERENCES drivers(driver_name), " +
                    "FOREIGN KEY (fastest_lap) REFERENCES drivers(driver_name))");

            System.out.println("Created racetracks_stats table\n");
            Thread.sleep(500);


        } catch (Exception e) {
            System.out.println("Error through process: " + e);
            Thread.sleep(5000);
        }
    }

    public void performQueries() throws SQLException, InterruptedException {
        // Queries
        // Query 1
        // Getting the driver with the most wins in the 2023 season
        System.out.println("Query 1: Find the driver with the most wins in the 2023 season:\n");
        ResultSet rs = statement.executeQuery("SELECT d.driver_name, d.constructor, d.racer_number, ds.points, ds.podiums, ds.wins, ds.pole_positions, ds.fastest_laps FROM drivers d JOIN drivers_stats ds ON d.driver_name = ds.driver_name ORDER BY ds.wins DESC");
        System.out.println("Drivers:");
        System.out.printf("| %-16s | %-28s | %-12s | %-6s | %-8s | %-4s | %-14s | %-12s |\n", "Driver Name", "Team", "Racer Number", "Points", "Podiums", "Wins", "Pole Positions", "Fastest Laps");
        System.out.printf("| %-16s | %-28s | %-12s | %-6s | %-8s | %-4s | %-14s | %-12s |\n", "----------------", "----------------------------", "------------", "------", "--------", "----", "--------------", "------------");
        while (rs.next()) {
            System.out.printf("| %-16s | %-28s | %-12s | %-6d | %-8d | %-4d | %-14d | %-12d |\n",
                    rs.getString("driver_name"),
                    rs.getString("constructor"),
                    rs.getInt("racer_number"),
                    rs.getInt("points"),
                    rs.getInt("podiums"),
                    rs.getInt("wins"),
                    rs.getInt("pole_positions"),
                    rs.getInt("fastest_laps"));
        }
        Thread.sleep(1000);

        // Query 2
        // Getting all races that took place in the 2023 season ordered by date
        System.out.println("\n\nQuery 2: Find all the races that took place in the 2023 season ordered by date:\n");
        ResultSet rs2 = statement.executeQuery("SELECT round, grand_prix, country, continent, circuit, race_date FROM racetracks ORDER BY race_date ASC");
        System.out.println("Racetracks:");
        System.out.printf("| %-5s | %-25s | %-24s | %-9s | %-44s | %-10s |\n", "Round", "Grand Prix", "Country", "Continent", "Circuit", "Race Date");
        System.out.printf("| %-5s | %-25s | %-24s | %-9s | %-44s | %-10s |\n", "-----", "-------------------------", "------------------------", "---------", "--------------------------------------------", "----------");
        while (rs2.next()) {
            System.out.printf("| %-5d | %-25s | %-24s | %-9s | %-44s | %-10s |\n",
                    rs2.getInt("round"),
                    rs2.getString("grand_prix"),
                    rs2.getString("country"),
                    rs2.getString("continent"),
                    rs2.getString("circuit"),
                    rs2.getString("race_date"));
        }
        Thread.sleep(1000);


        // Query 3
        // Getting all teams of the season and ordering by points
        System.out.println("\n\nQuery 3: Getting all teams of the season and ordering by points:\n");
        ResultSet rs3 = statement.executeQuery("SELECT constructor, chassis, power_unit, team_principal, headquarters, year_of_entry, points, world_championships FROM driving_team ORDER BY points DESC");
        System.out.println("Teams:");
        System.out.printf("| %-28s | %-7s | %-19s | %-30s | %-30s | %-13s | %-6s | %-19s |\n", "Team Name", "Chassis", "Power Unit", "Team Principal", "Headquarters", "Year of Entry", "Points", "World Championships");
        System.out.printf("| %-28s | %-7s | %-19s | %-30s | %-30s | %-13s | %-6s | %-19s |\n", "----------------------------", "-------", "-------------------", "------------------------------", "------------------------------", "-------------", "------", "-------------------");
        while (rs3.next()) {
            System.out.printf("| %-28s | %-7s | %-19s | %-30s | %-30s | %-13s | %-6d | %-19d |\n",
                    rs3.getString("constructor"),
                    rs3.getString("chassis"),
                    rs3.getString("power_unit"),
                    rs3.getString("team_principal"),
                    rs3.getString("headquarters"),
                    rs3.getString("year_of_entry"),
                    rs3.getInt("points"),
                    rs3.getInt("world_championships"));
        }
        Thread.sleep(1000);

        // Query 4
        // Get each driver along with its car and power unit
        System.out.println("\n\nQuery 4: Get each driver along with its car and power unit:\n");
        ResultSet rs4 = statement.executeQuery("SELECT d.racer_number, d.driver_name, dt.constructor AS team_name, dt.chassis AS chassis, dt.power_unit AS power_unit, ds.points, dt.points AS team_points FROM drivers d JOIN driving_team dt ON d.constructor = dt.constructor JOIN drivers_stats ds ON d.driver_name = ds.driver_name ORDER BY dt.points DESC");
        System.out.println("Driver-Team Relationship:");
        System.out.printf("| %-12s | %-16s | %-28s | %-7s | %-19s | %-13s | %-11s |\n", "Racer Number", "Driver Name", "Team Name", "Chassis", "Power Unit", "Driver points", "Team Points");
        System.out.printf("| %-12s | %-16s | %-28s | %-7s | %-19s | %-13s | %-11s |\n", "------------", "----------------", "----------------------------", "-------", "-------------------", "-------------", "-----------");
        while (rs4.next()) {
            System.out.printf("| %-12d | %-16s | %-28s | %-7s | %-19s | %-13d | %-11d |\n",
                    rs4.getInt("racer_number"),
                    rs4.getString("driver_name"),
                    rs4.getString("team_name"),
                    rs4.getString("chassis"),
                    rs4.getString("power_unit"),
                    rs4.getInt("points"),
                    rs4.getInt("team_points"));
        }
        Thread.sleep(1000);

        // Query 5
        // Get the top 5 drivers of the 2023 season
        System.out.println("\n\nQuery 5: Get the top 5 drivers of the 2023 season:\n");
        ResultSet rs5 = statement.executeQuery("SELECT d.driver_name, d.constructor, d.racer_number, ds.points, ds.podiums, ds.wins, ds.pole_positions, ds.fastest_laps FROM drivers d JOIN drivers_stats ds ON d.driver_name = ds.driver_name ORDER BY ds.points DESC LIMIT 5");
        System.out.println("Top 5 Drivers:");
        System.out.printf("| %-12s | %-16s | %-28s | %-6s | %-7s | %-4s | %-14s | %-12s |\n", "Racer Number", "Driver Name", "Team", "Points", "Podiums", "Wins", "Pole Positions", "Fastest Laps");
        System.out.printf("| %-12s | %-16s | %-28s | %-6s | %-7s | %-4s | %-14s | %-12s |\n", "------------", "----------------", "----------------------------", "------", "-------", "----", "--------------", "------------");
        while (rs5.next()) {
            System.out.printf("| %-12d | %-16s | %-28s | %-6d | %-7d | %-4d | %-14d | %-12d |\n",
                    rs5.getInt("racer_number"),
                    rs5.getString("driver_name"),
                    rs5.getString("constructor"),
                    rs5.getInt("points"),
                    rs5.getInt("podiums"),
                    rs5.getInt("wins"),
                    rs5.getInt("pole_positions"),
                    rs5.getInt("fastest_laps"));
        }
        Thread.sleep(1000);

        // Query 6
        // Counting racetracks by continent
        System.out.println("\n\nQuery 6: Counting racetracks by continent:\n");
        ResultSet rs6 = statement.executeQuery("SELECT continent, COUNT(*) AS circuits FROM racetracks GROUP BY continent");
        System.out.println("Racetracks by Continent:");
        System.out.printf("| %-9s | %-8s |\n", "Continent", "Circuits");
        System.out.printf("| %-9s | %-8s |\n", "---------", "--------");
        while (rs6.next()) {
            System.out.printf("| %-9s | %-8d |\n",
                    rs6.getString("continent"),
                    rs6.getInt("circuits"));
        }
        Thread.sleep(1000);

        // Query 7
        // Get the top 5 fastest laps and driver from each racetrack
        System.out.println("\n\nQuery 7: Get the top 5 fastest laps and driver from each racetrack:\n");
        ResultSet rs7 = statement.executeQuery("SELECT grand_prix, fastest_lap, fastest_lap_time FROM racetracks_stats ORDER BY fastest_lap_time ASC");
        System.out.println("Fastest Laps:");
        System.out.printf("| %-25s | %-16s | %-16s |\n", "Grand Prix", "Fastest Lap By", "Fastest Lap Time");
        System.out.printf("| %-25s | %-16s | %-16s |\n", "-------------------------", "----------------", "----------------");
        while (rs7.next()) {
            System.out.printf("| %-25s | %-16s | %-16s |\n",
                    rs7.getString("grand_prix"),
                    rs7.getString("fastest_lap"),
                    rs7.getString("fastest_lap_time"));
        }
        Thread.sleep(1000);

        // Query 8
        // Get average fastest lap time
        System.out.println("\n\nQuery 8: Get average fastest lap time\n");
        ResultSet rs8 = statement.executeQuery("SELECT AVG((substr(fastest_lap_time, 1, instr(fastest_lap_time, ':') - 1) * 60 + substr(fastest_lap_time, instr(fastest_lap_time, ':') + 1, instr(fastest_lap_time, '.') - instr(fastest_lap_time, ':') - 1) + substr(fastest_lap_time, instr(fastest_lap_time, '.') + 1) / 1000.0)) AS average_lap_time_seconds FROM racetracks_stats");
        System.out.println("Average Fastest Lap Time:");
        System.out.printf("| %-28s |\n", "Average Lap Time (MM:SS.sss)");
        System.out.printf("| %-28s |\n", "----------------------------");
        while (rs8.next()) {
            double averageLapTimeSeconds = rs8.getDouble("average_lap_time_seconds");
            int minutes = (int) (averageLapTimeSeconds / 60);
            double fractionalSeconds = averageLapTimeSeconds % 60;
            int seconds = (int) fractionalSeconds;
            int milliseconds = (int) ((fractionalSeconds - seconds) * 1000);
            System.out.printf("| %02d:%02d.%03d %-18s |\n", minutes, seconds, milliseconds, "");
        }
        Thread.sleep(1000);

        // Query 9
        // Order race engineers by experience with their drivers
        System.out.println("\n\nQuery 9: Getting race engineers with their drivers by experience\n");
        ResultSet rs9 = statement.executeQuery("SELECT e.engineer_name, e.years_with_driver, e.years_of_f1_entry, d.driver_name, e.constructor, ds.points FROM race_engineers e JOIN drivers d ON e.engineer_name = d.race_engineer JOIN drivers_stats ds ON d.driver_name = ds.driver_name ORDER BY e.years_of_f1_entry ASC");
        System.out.println("Race engineers by experience: ");
        System.out.printf("| %-20s | %-24s | %-16s | %-16s | %-28s | %-6s |\n", "Engineer Name", "Year Started With Driver", "Year of F1 Entry", "Driver managed", "Team", "Points");
        System.out.printf("| %-20s | %-24s | %-16s | %-16s | %-28s | %-6s |\n", "--------------------", "------------------------", "----------------", "----------------", "----------------------------", "------");
        while (rs9.next()) {
            System.out.printf("| %-20s | %-24d | %-16d | %-16s | %-28s | %-6d |\n",
                    rs9.getString("engineer_name"),
                    rs9.getInt("years_with_driver"),
                    rs9.getInt("years_of_f1_entry"),
                    rs9.getString("driver_name"),
                    rs9.getString("constructor"),
                    rs9.getInt("points"));
        }
        Thread.sleep(1000);

        // Query 10
        // Grouping drivers by countries of origin
        System.out.println("\n\nQuery 10: Grouping drivers by countries of origin\n");
        ResultSet rs10 = statement.executeQuery("SELECT country_of_birth, COUNT(*) AS drivers FROM drivers GROUP BY country_of_birth UNION ALL SELECT 'Total', COUNT(*) FROM drivers");
        System.out.println("Drivers by Country of Origin:");
        System.out.printf("| %-24s | %-7s |\n", "Country", "Drivers");
        System.out.printf("| %-24s | %-7s |\n", "------------------------", "-------");
        while (rs10.next()) {
            String country = rs10.getString("country_of_birth");
            System.out.printf("| %-24s | %-7d |\n", country, rs10.getInt("drivers"));
        }
        Thread.sleep(1000);

        // Query 11
        // Get drivers that entered before 2015 and have won at least one world championship
        System.out.println("\n\nQuery 11: Getting drivers that entered before 2015 and have won at least one world championship\n");
        ResultSet rs11 = statement.executeQuery("SELECT d.racer_number, d.driver_name, d.year_of_entry, ds.world_championships FROM drivers d JOIN drivers_stats ds ON d.driver_name = ds.driver_name WHERE d.year_of_entry < 2015 AND ds.world_championships > 0 ORDER BY ds.world_championships DESC");
        System.out.println("Drivers that entered before 2015 and have won at least one world championship:");
        System.out.printf("| %-12s | %-16s | %-13s | %-19s |\n", "Racer Number", "Driver Name", "Year of Entry", "World Championships");
        System.out.printf("| %-12s | %-16s | %-13s | %-19s |\n", "------------","----------------", "-------------", "-------------------");
        while (rs11.next()) {
            System.out.printf("| %-12d | %-16s | %-13s | %-19d |\n",
                    rs11.getInt("racer_number"),
                    rs11.getString("driver_name"),
                    rs11.getString("year_of_entry"),
                    rs11.getInt("world_championships"));
        }
        Thread.sleep(1000);

        // Query 12
        // Get teams, drivers and engineers
        System.out.println("\n\nQuery 12: Getting teams, drivers and race engineers\n");
        ResultSet rs12 = statement.executeQuery("WITH TeamDriverEngineer AS (SELECT dt.constructor AS team_name, dt.team_principal, re.engineer_name, d.driver_name, ROW_NUMBER() OVER (PARTITION BY dt.constructor, d.driver_name ORDER BY d.driver_name) AS row_num FROM driving_team dt JOIN race_engineers re ON dt.constructor = re.constructor JOIN drivers d ON re.engineer_name = d.race_engineer) SELECT team_name, team_principal, engineer_name, driver_name FROM TeamDriverEngineer WHERE row_num <= 2 ORDER BY team_name, driver_name");
        System.out.println("Teams, Team Principals, Drivers and Race Engineers:");
        System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n", "Team Name", "Team Principal", "Drivers", "Race Engineers");
        System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n", "----------------------------", "------------------------------", "----------------", "--------------------");
        while (rs12.next()) {
            System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n",
                    rs12.getString("team_name"),
                    rs12.getString("team_principal"),
                    rs12.getString("driver_name"),
                    rs12.getString("engineer_name"));
        }
        Thread.sleep(1000);

        // Query 13
        // Get teams, drivers and engineers after deletion
        System.out.println("\n\nNyck de Vries was fired from Formula 1 mid-season, and replaced by Daniel Ricciardo. \nAlso, Liam Lawson is a reserve driver, DB means to keep integrity on the main drivers, so deleting both from database\n");
        statement.executeUpdate("DELETE FROM drivers WHERE driver_name = 'Nyck de Vries' OR driver_name = 'Liam Lawson'");
        Thread.sleep(500);

        System.out.println("Nick de Vries and Liam Lawson have been deleted from the database\n\nQuery 13: Updated table:");
        ResultSet rs13 = statement.executeQuery("WITH TeamDriverEngineer AS (SELECT dt.constructor AS team_name, dt.team_principal, re.engineer_name, d.driver_name, ROW_NUMBER() OVER (PARTITION BY dt.constructor, d.driver_name ORDER BY d.driver_name) AS row_num FROM driving_team dt JOIN race_engineers re ON dt.constructor = re.constructor JOIN drivers d ON re.engineer_name = d.race_engineer) SELECT team_name, team_principal, engineer_name, driver_name FROM TeamDriverEngineer WHERE row_num <= 2 ORDER BY team_name, driver_name");
        System.out.println("Teams, Team Principals, Drivers and Race Engineers:");
        System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n", "Team Name", "Team Principal", "Drivers", "Race Engineers");
        System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n", "----------------------------", "------------------------------", "----------------", "--------------------");
        while (rs13.next()) {
            System.out.printf("| %-28s | %-30s | %-16s | %-20s |\n",
                    rs13.getString("team_name"),
                    rs13.getString("team_principal"),
                    rs13.getString("driver_name"),
                    rs13.getString("engineer_name"));
        }
        Thread.sleep(1000);


        // Query 14
        // Delete Emilia Romagna Grand Prix
        System.out.println("\n\nQuery 14: The Emilia Romagna Grand Prix was cancelled due to floods in the region... Deleting from database\n");
        statement.executeUpdate("DELETE FROM racetracks WHERE grand_prix = 'Emilia Romagna Grand Prix'");
        System.out.println("Emilia Romagna Grand Prix has been deleted from the database\n\nUpdated table:");
        ResultSet rs14 = statement.executeQuery("SELECT round, grand_prix, country, continent, circuit, laps, race_date FROM racetracks");
        System.out.println("Racetracks:");
        System.out.printf("| %-5s | %-25s | %-24s | %-9s | %-34s | %-4s | %-12s |\n", "Round", "Grand Prix", "Country", "Continent", "Circuit", "Laps", "Date of Race");
        System.out.printf("| %-5s | %-25s | %-24s | %-9s | %-34s | %-4s | %-12s |\n", "-----", "-------------------------", "------------------------", "---------", "----------------------------------", "----", "------------");
        while (rs14.next()) {
            System.out.printf("| %-5d | %-25s | %-24s | %-9s | %-34s | %-4d | %-12s |\n",
                    rs14.getInt("round"),
                    rs14.getString("grand_prix"),
                    rs14.getString("country"),
                    rs14.getString("continent"),
                    rs14.getString("circuit"),
                    rs14.getInt("laps"),
                    rs14.getString("race_date"));
        }
        Thread.sleep(1000);
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
            Thread.sleep(500);

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

                for(int i = 1; i < allRows.size(); i++) {
                    String[] row = allRows.get(i);

                    for (int j = 0; j < numColumns; j++) {
                        data[j][i-1] = row[j];
                    }
                }

                System.out.println("Read " + file + " successfully!");
                System.out.println("Received " + headers.length + " columns and " + data[0].length + " rows. \nTable ready for upload!\n");
                Thread.sleep(500);

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


    // External class to upload data to database
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
                Thread.sleep(500);

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

                    for (int j = 0; j < headers.length; j++) {
                        query += "'" + dataRows[j][i] + "'";

                        if (j != headers.length - 1) {
                            query += ", ";
                        }
                    }
                    query += ")";

                    statement.executeUpdate(query);
                }

                System.out.println("Data uploaded to: " + tableToUpload + " successfully!, " + querycount + " queries sent.\n\n");
                Thread.sleep(500);

            } catch (Exception e) {
                System.out.println("Error through process: " + e);
                Thread.sleep(5000);

            }
        }
    }
}