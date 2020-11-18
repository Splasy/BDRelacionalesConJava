package proyecto;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.NoSuchElementException;
import java.util.Scanner;

import com.microsoft.sqlserver.jdbc.SQLServerException;

public class BDRelacionalesJava {

	// Conexion --------------------------------------------------------------------

	public static Connection conectarMYSQL() {

		String jdbcURL = "jdbc:mysql://localhost:3306/bdfutbol";
		String username = "root";
		String password = "";

		Connection connection = null; // declaracion de la conexion fuera del try catch para poder usarla mas adelante

		try {

			connection = DriverManager.getConnection(jdbcURL, username, password);
			if (connection != null) {
				System.out.println("\n[i] STATUS: Connecting database Mysql [" + connection + "] OK");
				System.out.println("Aitor Romojaro Mart�nez\n");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1); // Imprimimos el error como Exception y salimos del programa con error 1
		}

		return connection;
	}

	public static Connection conectarSQL() throws ClassNotFoundException, SQLException {
		Connection connection = null;
		String url = "";
		
		url = "jdbc:sqlserver://"; 
		String username = "sa_cliente"; //usuario
		String password = "diazdiaz1"; //password del usuario
		String serverName = "localhost";
		String databaseName = "bdFutbol"; //nombre de la base de datos
		
		 url=url + serverName +";"+"DataBaseName="+databaseName; 
		Class.forName( "com.microsoft.sqlserver.jdbc.SQLServerDriver" ); 
		
//		Class.forName( "com.microsoft.sqlserver.jdbc.SQLServerDriver" ); 
//		String url = "jdbc:sqlserver://localhost\\SQLEXPRESS:1433; databasename=bdFutbol";
//		String username = "sa_cliente";
//		String password = "diazdiaz1";
//
		try {
			connection = DriverManager.getConnection(url, username, password);
			if (connection != null) {
				System.out.println("\n[i] STATUS: Connecting database Sql [" + connection + "] OK");
				System.out.println("Aitor Romojaro Mart�nez\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		return connection;
	}

	public static Connection conectarAccess() throws ClassNotFoundException, SQLException {

		String url = "C:\\Users\\Usuario\\Desktop\\BDFutbol.accdb";
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		Connection connection = DriverManager.getConnection("jdbc:ucanaccess://" + url);

		try {

			connection = DriverManager.getConnection(url);
			if (connection != null) {
				System.out.println("\n[i] STATUS: Connecting database Sql [" + connection + "] OK");
				System.out.println("Aitor Romojaro Mart�nez\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		return connection;
	}

	// L�gica del programa
	// ------------------------------------------------------------------------
	public static void main(String[] args) throws SQLException, ClassNotFoundException, NoSuchElementException {
		System.out.println("# JDBC #");

		Scanner scanner = new Scanner(System.in);

		int db = 0;
		int option = 0;

		// ---

		// ---

		System.out.println("" + "[i] Seleccione la base de datos con la que trabajar�:" + "\n    [0] MYSQL"
				+ "\n    [1] SQL" + "\n    [2] Access");
		db = scanner.nextInt();

		while (true) {
			if (db == 0 || db == 1) {
				System.out.println("\n[>] MENU ---------- ");
				System.out.println("\n[1] Visualizar todos los datos de equipos y el nombre de la liga."
								 + "\n[2] Insertar equipo." 
								 + "\n[3] Modificar equipo." 
								 + "\n[4] Eliminar equipo."
								 + "\n[5] Procedimiento para visualizar todos los contratos."
								 + "\n[6] Procedimiento para insertar un equipo"
								 + "\n[7] Procedimiento para ver los futbolistas en activo"
								 + "\n[8] Función para ver la cantidad de meses "
		
								 + "\n[0] Salir.");

				option = scanner.nextInt();

				switch (option) {
				case 1:
					verEquipos(db);
					break;
				case 2:
					insertarEquipo(db);
					break;
				case 3:
					modEquipo(db);
					break;
				case 4:
					eliEquipo(db);
					break;
				case 5:
					PVerContrato(db);
					break;
				case 6:
					PInsertar(db);
					break;
				case 7:
					PFutbolistasActivo(db);
					break;
				case 8:
					FCantidadMeses(db);
					break;

				default:
					System.out.println("Saliendo del programa...");
					System.exit(0);
					break;
				}
			} else if (db == 2) {
				System.out.println("\n[>] MENU ---------- ");
				System.out.println(
						"\n[1] Visualizar todos los datos de equipos y el nombre de la liga." 
					  + "\n[2] Insertar equipo."
					  + "\n[3] Modificar equipo." 
					  + "\n[4] Eliminar equipo." 
					  + "\n[0] Salir.");

				option = scanner.nextInt();

				switch (option) {
				case 1:
					verEquipos(db);
					break;
				case 2:
					insertarEquipo(db);
					break;
				case 3:
					modEquipo(db);
					break;
				case 4:
					eliEquipo(db);
					break;

				default:
					System.out.println("Saliendo del programa...");
					System.exit(0);
					break;
				}

			}
		}

	}

	// -------------------------------------------------------------------------------------------

	public static void verEquipos(int db) throws ClassNotFoundException, SQLException {

		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		Scanner scanner = new Scanner(System.in);

		String consulta = "SELECT *, ligas.nomLiga FROM equipos inner join ligas on ligas.codLiga = equipos.codLiga";

		PreparedStatement preparedstmt;
		ResultSet rs;

		try {
			if (con != null) {

				preparedstmt = con.prepareStatement(consulta);

				preparedstmt.execute();

				rs = preparedstmt.getResultSet();

				while (rs.next()) {

					System.out.println(rs.getString("codEquipo") + " " + rs.getString("nomEquipo") + "\t"
							+ rs.getString("codLiga") + " " + rs.getString("localidad") + "\t"
							+ rs.getString("internacional") + "\t" + rs.getString("nomLiga"));
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// -------------------------------------------------------------------------------------------

	public static void insertarEquipo(int db) throws SQLException, ClassNotFoundException {

		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		Scanner scanner = new Scanner(System.in);

		System.out.println("[>] Introduzca el nombre del equipo");
		String nomEquipo = scanner.nextLine();

		System.out.println("[>] Introduzca el código de liga");
		String codLiga = scanner.nextLine();

		System.out.println("[>] Introduzca el nombre de la localidad");
		String localidad = scanner.nextLine();

		System.out.println("[>] Introduzca si es internacional(1 = true o  0 = false)");
		int internacional = scanner.nextInt();

		String consulta = "insert into equipos(nomEquipo, codLiga, localidad, internacional) values (?, ?, ?, ?)";

//		PreparedStatement preparedstmt;
//		ResultSet rs;
//
//		try {
//			if (con != null) {
//
//				preparedstmt = con.prepareStatement(consulta);
//
//				preparedstmt.execute();
//
//				rs = preparedstmt.getResultSet();
//
//			}
//
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}

		if (nomEquipo == "") {

			System.out.println("[!] Equipo no válido");
			System.out.println("Saliendo del programa...");
			System.exit(1);
		} else {

			PreparedStatement inserta = con.prepareStatement(consulta);
			inserta.setString(1, nomEquipo);
			inserta.setString(2, codLiga);
			inserta.setString(3, localidad);
			inserta.setInt(4, internacional);

			inserta.executeUpdate();
			con.close();

		}

	}
	// ---------------------------------------------------------------------------------------------------------

	public static void eliEquipo(int db) throws SQLException, ClassNotFoundException, SQLServerException {// Preguntar samuel

		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		Scanner scanner = new Scanner(System.in);

		String confirmar = "Si";

		System.out.print("Introduce el codigo del equipo:");
		int codEquipo = scanner.nextInt();

		String MonstarTableSQL = "SELECT codEquipo, nomEquipo, codLiga , localidad, internacional FROM equipos where codequipo = "
				+ codEquipo;
		String DELETEVALUETableSQL = "Delete from equipos where codequipo = " + codEquipo;

		String contadorcontratos = "select codEquipo from contratos where codEquipo=" + codEquipo;
		String borrarcontratos = "delete from contratos where codEquipo=" + codEquipo;
		int contador = 0;
		try {

			Statement st = con.createStatement();
			ResultSet result = st.executeQuery(contadorcontratos);
			while (result.next()) {
				contador++;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(contador);
		try {

			Statement st = con.createStatement();
			ResultSet result = st.executeQuery(MonstarTableSQL);
			while (result.next()) {
				System.out.println(result.getInt("codEquipo") + " | " + result.getString("nomEquipo") + " | "
						+ result.getString("equipos.codLiga") + " | " + result.getString("localidad") + " | "
						+ result.getString("internacional"));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("¿Seguro que quieres borrarlo?");
		System.out.println("Ponga Si o NO");
		confirmar = scanner.next();
		if (confirmar.equals("Si")) {
			if (contador > 0) {
				System.out.println("El equipo tiene contrato.�Estas seguro que quiere borrarlo?");
				confirmar = scanner.next();
				if (confirmar.equals("Si")) {
					try {

						PreparedStatement preparedStatement = con.prepareStatement(borrarcontratos);

						preparedStatement.executeUpdate();

					} catch (SQLException e) {
						e.printStackTrace();
					}
					try {

						PreparedStatement preparedStatement = con.prepareStatement(DELETEVALUETableSQL);

						preparedStatement.executeUpdate();

						System.out.println("Registro borrado correctamente");
					} catch (SQLException e) {
						e.printStackTrace();
					}

				}

			} else {
				try {

					PreparedStatement preparedStatement = con.prepareStatement(DELETEVALUETableSQL);

					preparedStatement.executeUpdate();

					con.close();

					System.out.println("Registro borrado correctamente");
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
		con.close();
	}

	// -------------------------------------------------------------------------------------------
	public static void modEquipo(int db) throws SQLException, ClassNotFoundException {

		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		Scanner scanner = new Scanner(System.in);

		System.out.println("[>] Introduzca el código del equipo a modificar");
		int codEquipo = scanner.nextInt();

		PreparedStatement preparedstmt = con.prepareStatement("select * from equipos where codEquipo = ?;");

		preparedstmt.setInt(1, codEquipo);

		ResultSet rs = preparedstmt.executeQuery();

		rs.next();

		System.out.println("Nombre equipo: " + rs.getString(2));
		System.out.println("C�digo liga: " + rs.getString(3));
		System.out.println("Localidad: " + rs.getString(4));
		System.out.println("Internacional: " + rs.getBoolean(5));

		System.out.println("Rellena solo los campos a modificar:");
		System.out.println(">----------------------------------------------------");

		scanner.nextLine();

		System.out.println("[>] Nombre equipo:");
		String nomEquipo = scanner.next();
		if (nomEquipo.equals("")) {
			nomEquipo = rs.getString("equipos.nomEquipo");
		}
		scanner.nextLine();

		System.out.println("[>] C�digo liga:");
		String codLiga = scanner.nextLine();
		if (codLiga.equals("")) {
			codLiga = rs.getString("equipos.codLiga");
		}

		System.out.println("[>] Localidad");
		String localidad = scanner.nextLine();
		if (localidad.equals("")) {
			localidad = rs.getString("equipos.localidad");
		}

		System.out.println("[>] Internacional (True/False):");
		String internacional = scanner.nextLine();

		if (internacional.equals("")) {
			internacional = String.valueOf(rs.getBoolean(5));
		}

		PreparedStatement modificar = con.prepareStatement(
				"UPDATE equipos SET nomEquipo=?,codLiga=?,localidad=?,internacional=? WHERE codEquipo=?;");

		modificar.setString(1, nomEquipo);
		modificar.setString(2, codLiga);
		modificar.setString(3, localidad);
		modificar.setBoolean(4, Boolean.parseBoolean(internacional));
		modificar.setInt(5, codEquipo);

		modificar.execute();

		con.close();
	}

	// -------------------------------------------------------------------------------------------
	public static void PVerContrato(int db) throws ClassNotFoundException, SQLException {

		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		String dni;
		Scanner scanner = new Scanner(System.in);

		System.out.print("[>] DNI del futbolista: ");
		dni = scanner.next();

		CallableStatement cs;
		ResultSet rs;
		try {

			if (con != null) {

				// procedimiento
				cs = (CallableStatement) con.prepareCall("{call contrato_futbolista(?)}");
				cs.setString(1, dni);
				rs = cs.executeQuery();

				while (rs.next()) {
					System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + ","
							+ rs.getString(4) + "," + rs.getString(5) + "," + rs.getString(6) + "," + rs.getString(7));
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

	// -------------------------------------------------------------------------------------------

	public static void PInsertar(int db) throws ClassNotFoundException, SQLException {
		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		} else if (db == 2) {
			con = conectarAccess();
		}

		Scanner in = new Scanner(System.in);
		System.out.print("[>] Introduce el nombre del equipo:");
		String nomEquipo = in.nextLine();

		System.out.print("[>] Introduce el codigo de la liga:");
		String codLiga = in.nextLine();

		System.out.print("[>] Introduce la localidad del equipo:");
		String localidad = in.nextLine();

		System.out.print("[>] Internacional:");
		int internacional = in.nextInt();

		try {
			CallableStatement proc = con.prepareCall(" CALL insertar_equipo(?,?,?,?,?,?)");
			proc.setString(1, nomEquipo);
			proc.setString(2, codLiga);
			proc.setString(3, localidad);
			proc.setInt(4, internacional);

			proc.registerOutParameter(5, Types.BIGINT);
			proc.registerOutParameter(6, Types.BIGINT);
			proc.execute();
			System.out.println("Liga existe: " + proc.getString(5));
			System.out.println("Equipo insertado: " + proc.getString(6));

			con.close();

			System.out.println("Registro insertado correctamente");

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	// -------------------------------------------------------------------------------------------

	public static void PFutbolistasActivo(int db) throws ClassNotFoundException, SQLException {
		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
			
			Scanner in = new Scanner(System.in);

			System.out.println("[>] Ingrese el código del equipo");
			int codEquipo = in.nextInt();
			System.out.println("[>] Ingrese el Precio inicial");
			int precioAnual = in.nextInt();
			System.out.println("[>] Ingrese el precio fin");
			int precioResicion = in.nextInt();

			try {

				CallableStatement proc = con.prepareCall("call sp_futbolistas (?,?,?,?,?)");
				proc.setInt(1, codEquipo);
				proc.setInt(2, precioAnual);
				proc.setInt(3, precioResicion);
				proc.registerOutParameter(4, Types.BIGINT);
				proc.registerOutParameter(5, Types.BIGINT);
				proc.execute();

				System.out.println("[>] Activo " + proc.getString(4));
				System.out.println("[>] Menor " + proc.getString(5));

				con.close();

				System.out.println("Registro encontrado correctamente");

			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (db == 1) {
			con = conectarSQL();
			
			Scanner in = new Scanner(System.in);

			System.out.println("[>] Ingrese el código del equipo");
			int codEquipo = in.nextInt();
			System.out.println("[>] Ingrese el Precio inicial");
			int precioAnual = in.nextInt();
			System.out.println("[>] Ingrese el precio fin");
			int precioResicion = in.nextInt();

			try {

				CallableStatement proc = con.prepareCall("exec sp_futbolistas ?,?,?,?,?");
				proc.setInt(1, codEquipo);
				proc.setInt(2, precioAnual);
				proc.setInt(3, precioResicion);
				proc.registerOutParameter(4, Types.BIGINT);
				proc.registerOutParameter(5, Types.BIGINT);
				proc.execute();

				System.out.println("[>] Activo " + proc.getString(4));
				System.out.println("[>] Menor " + proc.getString(5));

				con.close();

				System.out.println("Registro encontrado correctamente");

			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (db == 2) {
			con = conectarAccess();
		}
		
	}

	public static void FCantidadMeses(int db) throws ClassNotFoundException, SQLException {
		Connection con = null;
		if (db == 0) {
			con = conectarMYSQL();
		} else if (db == 1) {
			con = conectarSQL();
		}

		int total_sanciones2 = 0;
		int dni;
		Scanner scanner = new Scanner(System.in);

		System.out.print("[>] DNI: ");
		dni = scanner.nextInt();

		CallableStatement cs;

		try {

			if (con != null) {

				// funcion
				cs = con.prepareCall("{ ? = call f_mesesEnEquipos(?)}");

				cs.registerOutParameter(1, Types.SMALLINT);
				cs.setInt(2, dni);

				cs.execute();

				total_sanciones2 = cs.getInt(1);

			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		System.out.println(
				" [ " + dni + " ] : " + "\n -> Número de meses:  " + total_sanciones2);
	}
}
