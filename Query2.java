package src2;
import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Runs queries against a back-end database
 */
public class Query2
{
  private List<Itinerary> itineraries = new ArrayList<>();
  public int iid = 0;
	
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique

  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;
  
  private static final String CLEAR_TABLES = "DELETE FROM reservations; DELETE FROM users; DELETE FROM rid;";
  private PreparedStatement clearTables;
  
  private static final String CREATE_NEW_USER = "INSERT INTO users(username, password, balance) VALUES(?, ?, ?)";
  private PreparedStatement createNewUserStatement;
  
  private static final String CHECK_USER_EXISTS = "SELECT * FROM users WHERE username = ?";
  private PreparedStatement checkUserExistsStatement;
  
  private static final String ATTEMPT_USER_LOGIN = "SELECT * FROM users WHERE username = ? AND password = ?";
  private PreparedStatement attemptUserLogin;
  
  private static final String FIND_DIRECT_FLIGHTS = "SELECT TOP (?) fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price "
          + "FROM flights "
          + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0"
          + "ORDER BY actual_time ASC, fid ASC";
  private PreparedStatement findDirectFlights;
  
  private static final String FIND_INDIRECT_FLIGHTS = "SELECT TOP (?) f1.fid, f1.day_of_month, f1.carrier_id, f1.flight_num, f1.origin_city, f1.dest_city, f1.actual_time, f1.capacity, f1.price, "
		  														 + "f2.fid as fid2, f2.day_of_month as day_of_month2, f2.carrier_id as carrier_id2, f2.flight_num as flight_num2, f2.origin_city as origin_city2, f2.dest_city as dest_city2, f2.actual_time as actual_time2, f2.capacity as capacity2, f2.price as price2 "
          + "FROM flights f1, flights f2 "
          + "WHERE f1.origin_city = ? AND f1.dest_city = f2.origin_city AND f2.dest_city = ? AND f2.dest_city != ? AND f1.day_of_month = ? "
          + "AND f2.day_of_month = f1.day_of_month AND f2.canceled = 0 AND f2.canceled = 0 ";
  private PreparedStatement findIndirectFlights;
  
  private static final String FIND_USER_BOOKINGS = "SELECT * FROM reservations WHERE userReserved = ?";
  private PreparedStatement findUserBookings;
  
  private static final String BOOK_FLIGHT = "INSERT INTO reservations(rid, fid1, fid2, paid, userReserved) "
  											+ "VALUES(?, ?, ?, ?, ?)";
  private PreparedStatement bookFlight;
  
  private static final String GET_RESERVATION_ID = "SELECT * FROM rid";
  private PreparedStatement getReservationID;
  
  private static final String UPDATE_RESERVATION_ID = "UPDATE rid SET rid = ?";
  private PreparedStatement updateReservationID;
  
  private static final String INITIALIZE_RID = "INSERT INTO rid(rid) VALUES(1)";
  private PreparedStatement initializeRid; 
  
  private static final String FIND_FLIGHT_INFO = "SELECT * FROM flights where fid = ?";
  private PreparedStatement findFlightInfo;
  
  private static final String FIND_USER_RESERVATIONS = "SELECT * FROM reservations WHERE userReserved = ? ORDER BY rid ASC";
  private PreparedStatement findUserReservations;
  
  private static final String UPDATE_USER_BALANCE = "UPDATE users SET balance = ? WHERE username = ?";
  private PreparedStatement updateUserBalance;
  
  private static final String UPDATE_PAID_RESERVATION = "UPDATE reservations SET paid = 1 WHERE rid = ?";
  private PreparedStatement updatePaidReservation;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;
    
    public Flight(int fid, int dayOfMonth, String carrierId, String flightNum, String originCity, String destCity, 
    				  int time, int capacity, int price) {
    		this.fid = fid;
    		this.dayOfMonth = dayOfMonth;
    		this.carrierId = carrierId;
    		this.flightNum = flightNum;
    		this.originCity = originCity;
    		this.destCity = destCity;
    		this.time = time;
    		this.capacity = capacity;
    		this.price = price;
    }

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }
  
  class Itinerary {// implements Comparator<Itinerary> {
	  public int id;
	  public Flight flight1;
	  public Flight flight2;
	  
	  public Itinerary(Flight f1, int id) {
		  this.id = id;
		  this.flight1 = f1;	  
	  }
	  
	  public Itinerary(Flight f1, Flight f2, int id) {
		  this.id = id;
		  this.flight1 = f1;
		  this.flight2 = f2;
	  }
	  
	  public int getTotalFlightTime() {
		  try {
			  int totalTime = 0;
			  findFlightInfo.clearParameters();
			  findFlightInfo.setInt(1, flight1.fid);
			  ResultSet time1 = findFlightInfo.executeQuery();
			  time1.next();
			  totalTime += time1.getInt("actual_time");
			  time1.close();
			  
			  if (flight2 != null) {
				  findFlightInfo.clearParameters();
				  findFlightInfo.setInt(1, flight2.fid);
				  ResultSet time2 = findFlightInfo.executeQuery();
				  time2.next();
				  totalTime += time2.getInt("actual_time");
				  time2.close();
			  }
			  return totalTime;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	  }
	  
	  public int getNumFlights() {
		  if (flight2 != null) {
			  return 2;
		  }
		  return 1;
	  }
  }

  public Query2(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
    try {
    		
		clearTables.clearParameters();
		clearTables.executeUpdate();
		
		updateReservationID.clearParameters();
		updateReservationID.setInt(1, 1);
		updateReservationID.executeUpdate();
		
	} catch (SQLException e) { 
		
	}

  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
    createNewUserStatement = conn.prepareStatement(CREATE_NEW_USER);
    checkUserExistsStatement = conn.prepareStatement(CHECK_USER_EXISTS);
    attemptUserLogin = conn.prepareStatement(ATTEMPT_USER_LOGIN);
    findDirectFlights = conn.prepareStatement(FIND_DIRECT_FLIGHTS);
    findIndirectFlights = conn.prepareStatement(FIND_INDIRECT_FLIGHTS);
    clearTables = conn.prepareStatement(CLEAR_TABLES);
    findUserBookings = conn.prepareStatement(FIND_USER_BOOKINGS);
    bookFlight = conn.prepareStatement(BOOK_FLIGHT);
    getReservationID = conn.prepareStatement(GET_RESERVATION_ID);
    updateReservationID = conn.prepareStatement(UPDATE_RESERVATION_ID);
    initializeRid = conn.prepareStatement(INITIALIZE_RID);
    findFlightInfo = conn.prepareStatement(FIND_FLIGHT_INFO);
    findUserReservations = conn.prepareStatement(FIND_USER_RESERVATIONS);
    updateUserBalance = conn.prepareStatement(UPDATE_USER_BALANCE);
    updatePaidReservation = conn.prepareStatement(UPDATE_PAID_RESERVATION);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
	  
	  try {
		  beginTransaction();
		  // if username isn't null, it means someone is already logged in
		  if (this.username == null) {
			  // nobody is logged in yet
			  attemptUserLogin.clearParameters();
			  attemptUserLogin.setString(1, username);
			  attemptUserLogin.setString(2, password);
			  
			  ResultSet results = attemptUserLogin.executeQuery();
			  if (results.next()) {
				  // matching username and password found, so "log in"
				  this.username = username;
				  itineraries.clear(); // clear most recent itinerary searches on log in
				  iid = 0;
				  commitTransaction();
				  return "Logged in as " + username + "\n";
			  }
			  
			  results.close();
			  
			  // no match, login failed
			  rollbackTransaction();
			  return "Login failed\n";
		  }
		  // username not null, someone's logged in
		  rollbackTransaction();
	      return "User already logged in\n";

	  } catch (SQLException e) {
		  try {
			rollbackTransaction();
		} catch (SQLException e1) {}
		  return "Login failed]n";
	  }
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount)
  {   
	  try {
		  beginTransaction();
		  // check if user already exists
		  checkUserExistsStatement.clearParameters();
		  checkUserExistsStatement.setString(1, username);
		  ResultSet results = checkUserExistsStatement.executeQuery();
		  if (results.next() || initAmount < 0) {
			  // user already exists, because we retrieved someone with the same username from our db
			  rollbackTransaction();
			  return "cFailed to create user\n";
		  }
		  
		  // if we got to this point, the user doesn't exist so create it with given parameters
		  createNewUserStatement.clearParameters();
		  createNewUserStatement.setString(1, username);
		  createNewUserStatement.setString(2, password);
		  createNewUserStatement.setInt(3, initAmount);
			
		  if (createNewUserStatement.executeUpdate() == 0) {
			  rollbackTransaction();
			  return "bFailed to create user\n";
		  }
		  results.close();
		  commitTransaction();
	} catch (SQLException e1) {
		try {
			rollbackTransaction();
		} catch (Exception e2) {}
		return "aFailed to create user\n";
	}
    return "Created user " + username + "\n";
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {  
	  try {
		  beginTransaction();
		  
		  findDirectFlights.clearParameters();
		  findDirectFlights.setInt(1, numberOfItineraries);
		  findDirectFlights.setString(2, originCity);
		  findDirectFlights.setString(3, destinationCity);
		  findDirectFlights.setInt(4, dayOfMonth);
		  
		  ResultSet checkIfAnyFlights = findDirectFlights.executeQuery();
		  if (!checkIfAnyFlights.next()) {
			  rollbackTransaction();
			  return "No flights match your selection\n";
		  }
		  
		  ResultSet oneHopResults = findDirectFlights.executeQuery();
		  
		  // clear old results and populate set with new itineraryIds
		  itineraries.clear();
		  iid = 0;
		  
		  while (oneHopResults.next())
	      {
			
			int result_fid = oneHopResults.getInt("fid");
	        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
	        String result_carrierId = oneHopResults.getString("carrier_id");
	        String result_flightNum = oneHopResults.getString("flight_num");
	        String result_originCity = oneHopResults.getString("origin_city");
	        String result_destCity = oneHopResults.getString("dest_city");
	        int result_time = oneHopResults.getInt("actual_time");
	        int result_capacity = oneHopResults.getInt("capacity");
	        int result_price = oneHopResults.getInt("price");
	        
	        // update the most recent itinerary set with flights
	        Flight f1 = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, result_originCity, result_destCity, result_time, result_capacity, result_price);
	        Itinerary itinerary1hop = new Itinerary(f1, iid++);
	        itineraries.add(itinerary1hop);
	        
	      }
		  
		  oneHopResults.close();
		  
		  if (!directFlight) {
			  findIndirectFlights.clearParameters();
			  findIndirectFlights.setInt(1, numberOfItineraries);
			  findIndirectFlights.setString(2, originCity);
			  findIndirectFlights.setString(3, destinationCity);
			  findIndirectFlights.setString(4, originCity);
			  findIndirectFlights.setInt(5, dayOfMonth);
			  
			  ResultSet twoHopResults = findIndirectFlights.executeQuery();
			  while (twoHopResults.next()) {
				  int result_fid1 = twoHopResults.getInt("fid");
				  int result_dayOfMonth1 = twoHopResults.getInt("day_of_month");
				  String result_carrierId1 = twoHopResults.getString("carrier_id");
			      String result_flightNum1 = twoHopResults.getString("flight_num");
			      String result_originCity1 = twoHopResults.getString("origin_city");
			      String result_destCity1 = twoHopResults.getString("dest_city");
			      int result_time1 = twoHopResults.getInt("actual_time");
			      int result_capacity1 = twoHopResults.getInt("capacity");
			      int result_price1 = twoHopResults.getInt("price");
			      
			      int result_fid2 = twoHopResults.getInt("fid2");
			      int result_dayOfMonth2 = twoHopResults.getInt("day_of_month2");
				  String result_carrierId2 = twoHopResults.getString("carrier_id2");
			      String result_flightNum2 = twoHopResults.getString("flight_num2");
			      String result_originCity2 = twoHopResults.getString("origin_city2");
			      String result_destCity2 = twoHopResults.getString("dest_city2");
			      int result_time2 = twoHopResults.getInt("actual_time2");
			      int result_capacity2 = twoHopResults.getInt("capacity2");
			      int result_price2 = twoHopResults.getInt("price2");
			      
			      // update the most recent itinerary set with flights
			      // TODO: remove System.err.println("two hop flight time 1: " + result_time1);
			      // TODO: remove System.err.println("two hop flight time 2: " + result_time2);
			      Flight f1 = new Flight(result_fid1, result_dayOfMonth1, result_carrierId1, result_flightNum1, result_originCity1, result_destCity1, result_time1, result_capacity1, result_price1);
			      Flight f2 = new Flight(result_fid2, result_dayOfMonth2, result_carrierId2, result_flightNum2, result_originCity2, result_destCity2, result_time2, result_capacity2, result_price2);
			      Itinerary itinerary2hops = new Itinerary(f1 ,f2, iid++);
			      itineraries.add(itinerary2hops);
				  
			  }
		        twoHopResults.close();
		   }
		  
		  // all itineraries have been added, now we sort them and output in order of total time
		  sortItineraries();
		  
		   // now output itinerary results
		   StringBuffer sb = new StringBuffer();
		   int currentDisplayed = 0;
		   int itineraryNumber = 0;
		   
		   for (int i = 0; i < itineraries.size(); i++) {
			   // find itinerary time and number of flights
			   Itinerary itin = itineraries.get(i);
			   int totalTime = itin.getTotalFlightTime();
			   int numFlights = itin.getNumFlights();
			   
			   // display results
			   sb.append("Itinerary " + itineraryNumber++ + ": " + numFlights + " flight(s), " + totalTime + " minutes\n");   
			   sb.append(itin.flight1.toString() + "\n");
			   if (itin.flight2 != null) {
				   sb.append(itineraries.get(i).flight2.toString() + "\n");
			   }
			   currentDisplayed++;
			   
			   // break when we list top n results
			   if (currentDisplayed == numberOfItineraries) {
				   break;
			   }
		   }
		  
		   commitTransaction();
		   return sb.toString();
		   
	  } catch (SQLException e) {
		  try {
			rollbackTransaction();
		} catch (SQLException e1) {}
		  return "Failed to search\n";
	  }
	  
  }
  
//  /**
//   * Same as {@code transaction_search} except that it only performs single hop search and
//   * do it in an unsafe manner.
//   *
//   * @param originCity
//   * @param destinationCity
//   * @param directFlight
//   * @param dayOfMonth
//   * @param numberOfItineraries
//   *
//   * @return The search results. Note that this implementation *does not conform* to the format required by
//   * {@code transaction_search}.
//   */
//  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
//                                          int dayOfMonth, int numberOfItineraries)
//  {
//    StringBuffer sb = new StringBuffer();
//
//    try
//    {
//      // one hop itineraries
//      String unsafeSearchSQL =
//              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
//                      + "FROM Flights "
//                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
//                      + "ORDER BY actual_time ASC";
//
//      Statement searchStatement = conn.createStatement();
//      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);
//
//      while (oneHopResults.next())
//      {
//        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
//        String result_carrierId = oneHopResults.getString("carrier_id");
//        String result_flightNum = oneHopResults.getString("flight_num");
//        String result_originCity = oneHopResults.getString("origin_city");
//        String result_destCity = oneHopResults.getString("dest_city");
//        int result_time = oneHopResults.getInt("actual_time");
//        int result_capacity = oneHopResults.getInt("capacity");
//        int result_price = oneHopResults.getInt("price");
//
//        sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
//      }
//      oneHopResults.close();
//    } catch (SQLException e) { e.printStackTrace(); }
//
//    return sb.toString();
//  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {  
	  try {
		  beginTransaction();
		  
		  // check if logged in
		  if (username == null) {
			  rollbackTransaction();
			  return "Cannot book reservations, not logged in\n";
		  }
		  
		  // itinerary must be in the set of most recent search results
		  for (Itinerary i: itineraries) {
			  if (i.id == itineraryId) {
				  // check if user has other bookings on the same day
					findUserBookings.clearParameters();
					findUserBookings.setString(1, username);
					ResultSet userBookings = findUserBookings.executeQuery();
					while (userBookings.next()) {
						
						int fid = userBookings.getInt("fid1");
						
						// flight has open seats, continue with booking
						findFlightInfo.clearParameters();
						findFlightInfo.setInt(1, fid);
						ResultSet userInfo = findFlightInfo.executeQuery();
						userInfo.next();
						int dayID = userInfo.getInt("day_of_month");
						
						if (dayID == i.flight1.dayOfMonth) {
							// user has a booking on the same day
							rollbackTransaction();
							return "You cannot book two flights in the same day\n";
						}
					}
					
					userBookings.close();
					
					/// we made it here, so there were no same-day bookings. Book the flight now! ///
					
					// but only if it isn't full
					int fid = userBookings.getInt("fid1");
					
					// check if flight is already full
//					int openSeats = checkFlightCapacity(fid);
//					if (openSeats < 1) {
//						rollbackTransaction();
//						return "Booking failed\n";
//					}
					
					// check if rid table has been initialized. if not, do it
					ResultSet isResult = getReservationID.executeQuery();
					if (!isResult.next()) {
						initializeRid.executeUpdate();
					}
					isResult.close();
					// get the current rID from the rID table
					ResultSet currentRid = getReservationID.executeQuery();
					currentRid.next();
					int rid = currentRid.getInt("rid");
					currentRid.close();
					
					// fill in book flight query
					bookFlight.clearParameters();
					bookFlight.setInt(1, rid);
					bookFlight.setInt(2, i.flight1.fid);
					if (i.flight2 != null) {
						bookFlight.setInt(3, i.flight2.fid);
					} else {
						bookFlight.setNull(3, Types.NULL);
					}
					bookFlight.setInt(4, 0);
					bookFlight.setString(5, username);
					bookFlight.executeUpdate();
					
					// update rID by incrementing one
					updateReservationID.clearParameters();
					updateReservationID.setInt(1, ++rid);
					updateReservationID.executeUpdate();
					
					// update flight capacities to be one less
//					updateFlightCapacity.clearParameters();
//					updateFlightCapacity.setInt(1, --openSeats);
//					updateFlightCapacity.setInt(2, i.flight1.fid);
//					if (i.flight2 != null) {
//						updateFlightCapacity.clearParameters();
//						updateFlightCapacity.setInt(1, --openSeats);
//						updateFlightCapacity.setInt(2, i.flight2.fid);
//					}
					
					commitTransaction();
					return "Booked flight(s), reservation ID: " + (rid - 1) + "\n";
			  }
		  }
		  
		  // looped through but didn't find matching itinerary
		  rollbackTransaction();
		  return "No such itinerary " + itineraryId + "\n";
		  
	  } catch (SQLException e) { 
		  try {
			rollbackTransaction();
		} catch (SQLException e1) {}
		  return "Booking failed\n";
	  }
			  
  }
	  

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
	  try {
		  beginTransaction();
		  // check if logged in
		  if (username == null) {
			  rollbackTransaction();
			  return "Cannot view reservations, not logged in\n";
		  }
		  
		findUserReservations.clearParameters();
		findUserReservations.setString(1, username);
		ResultSet userReservations = findUserReservations.executeQuery();
		if (!userReservations.next()) {
			rollbackTransaction();
			return "No reservations found\n";
		}
		StringBuffer resultsb = new StringBuffer();
		do {
			int paid = userReservations.getInt("paid");
			int fid1 = userReservations.getInt("fid1");
			int fid2 = userReservations.getInt("fid2");
			
			// get info on first flight
			findFlightInfo.clearParameters();
			findFlightInfo.setInt(1, fid1);
			ResultSet flightInfo1 = findFlightInfo.executeQuery();
			
			// for display
			
			StringBuffer sb = new StringBuffer();
			
			while (flightInfo1.next()) {
				 int fid = flightInfo1.getInt("fid");
				 int day = flightInfo1.getInt("day_of_month");
				 String carrierId = flightInfo1.getString("carrier_id");
				 String flightNum = flightInfo1.getString("flight_num");
				 String originCity = flightInfo1.getString("origin_city");
				 String destCity = flightInfo1.getString("dest_city");
				 int time = flightInfo1.getInt("actual_time");
				 int capacity = flightInfo1.getInt("capacity");
				 int price = flightInfo1.getInt("price");
				 
				 Flight f1 = new Flight(fid, day, carrierId, flightNum, originCity, destCity, time, capacity, price);
				 sb.append(f1.toString() + "\n");
			}
			flightInfo1.close();
			
			ResultSet flightInfo2;
			// check if there's a connecting flight
			if (fid2 != Types.NULL) {
				findFlightInfo.clearParameters();
				findFlightInfo.setInt(1, fid2);
				flightInfo2 = findFlightInfo.executeQuery();
				
				int fid = flightInfo2.getInt("fid");
				int day = flightInfo2.getInt("day_of_month");
				String carrierId = flightInfo2.getString("carrier_id");
			 	String flightNum = flightInfo2.getString("flight_num");
				String originCity = flightInfo2.getString("origin_city");
				String destCity = flightInfo2.getString("dest_city");
				int time = flightInfo2.getInt("actual_time");
				int capacity = flightInfo2.getInt("capacity");
				int price = flightInfo2.getInt("price");
				
				Flight f2 = new Flight(fid, day, carrierId, flightNum, originCity, destCity, time, capacity, price);
				sb.append(f2.toString() + "\n");
				flightInfo2.close();
			}
			
			// depending on if flight was direct or not, add to beginning string
			boolean isPaid = false;
			if (paid == 1) {
				isPaid = true;
			}
			
			int rid = userReservations.getInt("rid");
			resultsb.append("Reservation " + rid + " paid: " + isPaid + ":\n");
			resultsb.append(sb);
			
		} while (userReservations.next());
		
		userReservations.close();
		commitTransaction();
		return resultsb.toString();
		
	} catch (SQLException e) {
		try {
			rollbackTransaction();
		} catch (SQLException e2) {}
		return "Failed to retrieve reservations\n";
	}
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
    // only implement this if you are interested in earning extra credit for the HW!
    return "Failed to cancel reservation " + reservationId;
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
	  
	  try {
		  beginTransaction();
		  
		  // first check if logged in
		  if (username == null) {
			  rollbackTransaction();
			  return "Cannot pay, not logged in\n";
		  }
		  
		// see if user even has the reservation
		findUserReservations.clearParameters();
		findUserReservations.setString(1, username);
		ResultSet userReservations = findUserReservations.executeQuery();
		while (userReservations.next()) {
			int rid = userReservations.getInt("rid");
			if (rid == reservationId) {
				// we found a match!
				
				// check if match is paid
				int paid = userReservations.getInt("paid");
				if (paid == 1) {
					// it is already paid for
					rollbackTransaction();
					return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
				}
				
				// not paid, so check if user has enough money to pay for it
				// start out by getting user balance
				checkUserExistsStatement.clearParameters();
				checkUserExistsStatement.setString(1, username); // same query as getting user info, just a weird name
				ResultSet results = checkUserExistsStatement.executeQuery();
				results.next();
				int userBalance = results.getInt("balance");
				results.close();
				
				// now find price of flight(s)
				int fid1 = userReservations.getInt("fid1");
				int fid2 = userReservations.getInt("fid2");
				
				int totalPrice = 0;
				// find first price
				findFlightInfo.clearParameters();
				findFlightInfo.setInt(1, fid1);
				ResultSet f1info = findFlightInfo.executeQuery();
				f1info.next();
				totalPrice += f1info.getInt("price");
				
				// if exists, find second price
				if (fid2 != Types.NULL) {
					findFlightInfo.clearParameters();
					findFlightInfo.setInt(1, fid2);
					ResultSet f2info = findFlightInfo.executeQuery();
					f2info.next();
					totalPrice += f2info.getInt("price");
				}
				
				if (totalPrice > userBalance) {
					// insufficient fund to pay
					rollbackTransaction();
					return "User had only " + userBalance + " in account but itinerary costs " + totalPrice + "\n";
				}
				
				// we got here, so user must have enough money to pay for flight!
				
				// start out by updating user balance
				userBalance -= totalPrice;
				updateUserBalance.clearParameters();
				updateUserBalance.setInt(1, userBalance);
				updateUserBalance.setString(2, username);
				updateUserBalance.executeUpdate();
				
				// update reservation to be paid
				updatePaidReservation.clearParameters();
				updatePaidReservation.setInt(1, reservationId);
				updatePaidReservation.executeUpdate();
				
				commitTransaction();
				return "Paid reservation: " + reservationId + " remaining balance: " + userBalance + "\n";
			}
		}
		// if we get here, it means we didn't find the reservation under that user
		userReservations.close();
		rollbackTransaction();
		return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
		
	} catch (SQLException e) {
		try {
			rollbackTransaction();
		} catch (SQLException e2) {}
	    return "Failed to pay for reservation " + reservationId + "\n";
	}
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
  
  private void sortItineraries() {
	  Collections.sort(itineraries, new Comparator<Itinerary>() {
		    public int compare(Itinerary i1, Itinerary i2)  {
		    	try {
		    		
					  // get total time of first itinerary
					  int totalTime1 = 0;
					  findFlightInfo.clearParameters();
					  findFlightInfo.setInt(1, i1.flight1.fid);
					  ResultSet time1 = findFlightInfo.executeQuery();
					  time1.next();
					  totalTime1 += time1.getInt("actual_time");
					  time1.close();
					  
					  if (i1.flight2 != null) {
						  findFlightInfo.clearParameters();
						  findFlightInfo.setInt(1, i1.flight2.fid);
						  ResultSet time2 = findFlightInfo.executeQuery();
						  time2.next();
						  totalTime1 += time2.getInt("actual_time");
						  time2.close();
					  }
					  
					  // get total time of second itinerary
					  int totalTime2 = 0;
					  findFlightInfo.clearParameters();
					  findFlightInfo.setInt(1, i2.flight1.fid);
					  ResultSet time21 = findFlightInfo.executeQuery();
					  time21.next();
					  totalTime2 += time21.getInt("actual_time");
					  time21.close();
					  
					  if (i2.flight2 != null) {
						  findFlightInfo.clearParameters();
						  findFlightInfo.setInt(1, i1.flight2.fid);
						  ResultSet time22 = findFlightInfo.executeQuery();
						  time22.next();
						  totalTime2 += time22.getInt("actual_time");
						  time22.close();
					  }
					  
					  if (totalTime1 != totalTime2) {
						  // just compare by times
						  return totalTime1 - totalTime2;
					  }
					  
					  // those are equal, so now we have to sort by first flight id.
					  return i1.flight1.fid - i2.flight1.fid;
					  
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					return -1;
				}
		    }
		});
  }
  
  // check flight capacity?
  // two users book whatever
  
  // search "Seattle WA" "Boston MA" 1 1 10
}
