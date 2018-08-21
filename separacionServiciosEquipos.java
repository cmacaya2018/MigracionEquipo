package procesoMigracion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import procesoMigracion.dto.ServAsociadoDTO;
import procesoMigracion.dto.ServicioEquipoDTO;

public class separacionServiciosEquipos {
	
	public static void main(String argv[]) {
		
		System.out.println("Inicio del proceso");
	
		try {
			
			Connection conn = abreConexion("teleductos");

			limpiaTablaEquipos(conn);

			//Se obtienen todos los servicios.
			List <ServicioEquipoDTO> lServicios = obtieneListaServicioEquipos_ConModelo(conn);
			
//			conn.close();
//			conn = null;

			for (ServicioEquipoDTO servicio : lServicios) {
				
				if(servicio.getModeloNC() != null && !servicio.getModeloNC().equals("")){
					
//					conn = abreConexion("teleductos");
					
					//Se obtienen todos los equipos con sus enlaces asociados.
					List<ServAsociadoDTO> serviciosAsociados = getServiciosAsociadosEnlace(servicio,conn);
					
//					conn.close();
//					conn = null;
					
					System.out.println("codServicio: " +  servicio.getCodServicio());

					if (serviciosAsociados.size()<= 0) {
						
//						conn = abreConexion("teleductos");
						
						buscarDatosMigracion_ModeloEquipos_Standalone(conn, servicio);	
						if(servicio.getMigracionId() == null){
							buscarDatosMigracion_CaractValor(conn, servicio);
						}
						
						buscarDireccionDestinoEquipo(conn, servicio);
						
						insertServicioMigracionEquipo(servicio,null,servicio.getModelo(),servicio.getModeloNC(),
								servicio.getMigracionId(),servicio.getCodProducto(),"ERROR","EQUIPO SIN ENLACE ASOCIADO",
								servicio.getCodDireccionEquipo(),servicio.getDireccionEquipo(),conn);
						
						conn.commit();
//						conn.close();
//						conn = null;
						
					} else {
						
//						conn = abreConexion("teleductos");
						
						buscarDatosMigracion_ModeloEquipos(conn, servicio);	
						if(servicio.getMigracionId() == null){
							buscarDatosMigracion_CaractValor(conn, servicio);
						}
						
						buscarDireccionEquipo(conn, servicio);
								
						if(servicio.getModelo().contains("SWITCH")){
							
							if(serviciosAsociados.size() == 1){
								
								String clienteEnlace = getCliente(serviciosAsociados.get(0).getCodServicio(), conn);
								String clienteEquipo = getCliente(servicio.getCodServicioEquipo(), conn);
								buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(0).getCodServicio(), servicio);
								buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(0).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
								
								if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
							
									if(servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(0).getCodDireccionEnlaceOrigen()) ||
											servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(0).getCodDireccionEnlaceDestino())){
									
										insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", servicio.getCodDireccionEquipo(), 
												servicio.getDireccionEquipo(),conn);
									
									} else {
										
										//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE
										insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE",null,null,conn);
									}
									
								} else {
									
									//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
									insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",null,null,conn);
								}
								
							} else if(serviciosAsociados.size() == 2){
								
								if(esServicioPrincipal(serviciosAsociados.get(0).getCodServicio(), 
										serviciosAsociados.get(1).getCodServicio(), conn)){
									
									String clienteEnlace = getCliente(serviciosAsociados.get(0).getCodServicio(), conn);
									String clienteEquipo = getCliente(servicio.getCodServicioEquipo(), conn);
									buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(0).getCodServicio(), servicio);
									buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(0).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
									
									if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
									
										if(servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(0).getCodDireccionEnlaceOrigen()) ||
												servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(0).getCodDireccionEnlaceDestino())){
											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", servicio.getCodDireccionEquipo(), 
														servicio.getDireccionEquipo(),conn);
											
										} else {
											
											//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE
											insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE",null,null,conn);
										}
										
									} else {
										
										//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
										insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",null,null,conn);
									}
								
								} else if(esServicioPrincipal(serviciosAsociados.get(1).getCodServicio(), 
										serviciosAsociados.get(0).getCodServicio(), conn)){
									
									String clienteEnlace = getCliente(serviciosAsociados.get(1).getCodServicio(), conn);
									String clienteEquipo = getCliente(servicio.getCodServicioEquipo(), conn);
									buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(1).getCodServicio(), servicio);
									buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(1).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
									
									if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
										
										if(servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(1).getCodDireccionEnlaceOrigen()) ||
												servicio.getCodDireccionEquipo().equals(serviciosAsociados.get(1).getCodDireccionEnlaceDestino())){
											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", servicio.getCodDireccionEquipo(), 
														servicio.getDireccionEquipo(),conn);
											
										} else {
											
											//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE
											insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE",null,null,conn);
										}
									
									} else {
									
										//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
										insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",null,null,conn);
									}
									
								} else {
									
								}
								
							} else if(serviciosAsociados.size() > 2){
								
								//migrar Switch como offering “Equipamiento” (familia Otros de NC). PENDIENTE IT7
							}
							
						} else if(servicio.getModelo().contains("Router") || servicio.getModelo().contains("ROUTER")){
							
							if(serviciosAsociados.size() == 1){
								
								String clienteEnlace = getCliente(serviciosAsociados.get(0).getCodServicio(), conn);
								String clienteEquipo = getCliente(servicio.getCodServicioEquipo(), conn);
								buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(0).getCodServicio(), servicio);
								buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(0).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
								
								if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
								
									insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", servicio.getCodDireccionEquipo(), 
											servicio.getDireccionEquipo(),conn);
									
								} else {
									
									//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
									insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",null,null,conn);
								}
								
							} else if(serviciosAsociados.size() > 1){
								
								//migrar Switch como offering “Equipamiento” (familia Otros de NC). PENDIENTE IT7
							}
						}	
						
						conn.commit();
//						conn.close();
//						conn = null;
					}
					
				}/* else {
					
					conn = abreConexion("teleductos");
					
					insertServicioMigracionEquipo(servicio,null,null,null,null,servicio.getCodProducto(),"ERROR","EQUIPO SIN MODELO EN TABLA DE CONVERION NC",null,null,conn);
					
					conn.commit();
					conn.close();
					conn = null;
				}*/
			}
			
			conn.close();
			conn = null;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("Fin del proceso");		
	}

	private static boolean esServicioMigracion(ServicioEquipoDTO servicio, Connection conn) {
			
		boolean esServicioMigrado = false;
		
		try {
			String sql =
				"SELECT Sm.cod_servicio, sm.COD_SERVICIO_MIGRADO\n" +
				"  FROM teleductos.servicio_migracion2 sm\n" + 
				" WHERE sm.MIGRACION_ID IN\n" + 
				"       ('20000962', '20000871', '20000871', '10000045', '10001193',\n" + 
				"        '10001140', '10001474', '10002155', '10002295', '10001634',\n" + 
				"        '10002504', '10002437', '10002014', '10002014', '10002357')\n" + 
				"   AND sm.COD_SERVICIO = ?";

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodServicio());

			ResultSet rs = stmt.executeQuery();
	
			if (rs.next()) {
				esServicioMigrado = true;
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return esServicioMigrado;
	}
	
	private static List<ServAsociadoDTO> getServiciosAsociadosEnlace(ServicioEquipoDTO servicio, Connection conn) {
		
		List<ServAsociadoDTO> enlacesAsociados = new ArrayList<ServAsociadoDTO>();
		
		try {
			String sql =
				"SELECT\n" +
				"       s.COD_SERVICIO, s.cod_direccion_origen, s.cod_direccion_destino, pc.NOMBRE\n" + 
				"FROM\n" + 
				"       teleductos.SERVICIO_RELACION sr,\n" + 
				"       teleductos.servicio s,\n" + 
				"       comun.PRODUCTO_CLIENTE pc,\n" + 
				"       comun.SUBFAMILIA_PRODUCTO_CLIENTE sf,\n" + 
				"       comun.PRODUCTO_DETALLE pd\n" + 
				"WHERE\n" + 
				"       s.COD_PRODUCTO_CLIENTE = pd.COD_PRODUCTO_DETALLE AND\n" + 
				"       pc.COD_PRODUCTO = pd.COD_PRODUCTO AND\n" + 
				"       pc.COD_SUBFAMILIA = sf.COD_SUBFAMILIA AND\n" + 
				"       sr.TIPO_RELACION = 'ASO' and\n" + 
				"       sf.COD_FAMILIA IN (187,182) and\n" + 
				"       sr.COD_SERVICIO_RELACION = s.COD_SERVICIO AND\n" + 
				"       sr.COD_SERVICIO = ?\n" + 
				"UNION\n" + 
				"SELECT\n" + 
				"       s.COD_SERVICIO, s.cod_direccion_origen, s.cod_direccion_destino, pc.NOMBRE\n" + 
				"FROM\n" + 
				"       teleductos.SERVICIO_RELACION sr,\n" + 
				"       teleductos.servicio s,\n" + 
				"       comun.PRODUCTO_CLIENTE pc,\n" + 
				"       comun.SUBFAMILIA_PRODUCTO_CLIENTE sf,\n" + 
				"       comun.PRODUCTO_DETALLE pd\n" + 
				"WHERE\n" + 
				"       s.COD_PRODUCTO_CLIENTE = pd.COD_PRODUCTO_DETALLE AND\n" + 
				"       pc.COD_PRODUCTO = pd.COD_PRODUCTO AND\n" + 
				"       pc.COD_SUBFAMILIA = sf.COD_SUBFAMILIA AND\n" + 
				"       sr.TIPO_RELACION = 'ASO' and\n" + 
				"       sf.COD_FAMILIA IN (187,182) and\n" + 
				"       sr.COD_SERVICIO = s.COD_SERVICIO AND\n" + 
				"       sr.COD_SERVICIO_RELACION = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodServicio());
			stmt.setString(2, servicio.getCodServicio());
		
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {
				ServAsociadoDTO enlace = new ServAsociadoDTO();
				
				enlace.setCodServicio(rs.getString("COD_SERVICIO"));
				enlace.setCodDireccionEnlaceOrigen(rs.getString("cod_direccion_origen"));
				enlace.setCodDireccionEnlaceDestino(rs.getString("cod_direccion_destino"));
				
				enlacesAsociados.add(enlace);
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return enlacesAsociados;
	}
	
	private static String getCliente(String codServicio, Connection conn) {
		
		String cliente = null;
		
		try {
			String sql =
				"SELECT s.rut_cliente as CLIENTE FROM teleductos.servicio s WHERE s.COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, codServicio);
		
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {
				cliente = rs.getString("CLIENTE");
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return cliente;
	}
	
	private static boolean esEquipoAsociado(ServicioEquipoDTO servicio, Connection conn) {

		boolean esEquipoAsociado = false;
		
		try {
			
			String sql =
					"SELECT se.COD_SERVICIO, me.COD_MODELO, me.NOMBRE modelo\n" +
					"  FROM teleductos.SERVICIO_EQUIPO    se,\n" + 
					"       teleductos.MODELO_EQUIPO      me,\n" + 
					"       teleductos.EQUIPO_INSTALACION ei\n" + 
					" WHERE se.COD_EQUIPO = ei.COD_EQUIPO\n" + 
					"   AND ei.COD_MODELO = me.COD_MODELO\n" + 
					"   AND se.COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, servicio.getCodServicio());
		
			ResultSet rs = stmt.executeQuery();
	
			if (rs.next()) {
					esEquipoAsociado = true;
			}

			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return esEquipoAsociado;
	}
	
	private static void insertServicioMigracionEquipo(ServicioEquipoDTO servicio, String codServicioMigrado, String  modeloSge, String modeloNC,
			String migrationId, String codProducto, String estado, String descripcion, 
			String codDireccion, String direccion, Connection conn) {
		
		System.out.println("insertServicioMigracionEquipo");
		
		try {
			
		String sql =
				"INSERT INTO TELEDUCTOS.servicio_migracion_equipo2\n" +
						"  (COD_SERVICIO_MIGRADO,\n" + 
						"   COD_SERVICIO_EQUIPO,\n" + 
						"   MODELO_SGE,\n" + 
						"   MODELO_NC,\n" + 
						"   MIGRATION_ID,\n" + 
						"   COD_PRODUCTO,\n" + 
						"   USUARIO_CARGA,\n" + 
						"   FECHA_CARGA,\n" + 
						"   ESTADO,\n" + 
						"   DESCRIPCION_ERROR,\n" +
						"   COD_DIRECCION,\n" +
						"   DIRECCION,\n" +
						"	PRODUCT_CODE,\n" +
						"	ASSOCIATED_PRODUCTS,\n" +
						"	NUMERO_SERIE,\n" +
						"	MIGRATION_ID_PADRE)\n" +
						"VALUES\n" + 
						"  (?,\n" + 
						"   ?,\n" + 
						"   ?,\n" + 
						"   ?,\n" + 
						"   ?,\n" + 
						"   ?,\n" + 
						"   'SISTEMA',\n" + 
						"   sysdate,\n" + 
						"   ?,\n" + 
						"   ?,\n" +
						"   ?,\n" +
						"   ?,\n" +
						"   ?,\n" +
						"   ?,\n" +
						"   ?,\n" +
						"   ?)";

			PreparedStatement stmt = conn.prepareStatement(sql);

			stmt.setString(1, codServicioMigrado);
			stmt.setString(2, servicio.getCodServicio());
			stmt.setString(3, modeloSge);
			stmt.setString(4, modeloNC);
			stmt.setString(5, migrationId);
			stmt.setString(6, codProducto);
			stmt.setString(7, estado);
			stmt.setString(8, descripcion);
			stmt.setString(9, codDireccion);
			stmt.setString(10, direccion);
			stmt.setString(11, servicio.getProductCode());
			stmt.setString(12, servicio.getAssociatedProduct());
			stmt.setString(13, servicio.getNumeroSerie());
			stmt.setString(14, servicio.getMigracionIdServicioMigrado());

			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static List<ServicioEquipoDTO> obtieneListaServicioEquipos_ConModelo(Connection conn) {
		
		List <ServicioEquipoDTO> lServicios = new ArrayList<ServicioEquipoDTO>();
		
		try {
			String sql =
				"SELECT" +    
				"	  S.cod_servicio," +      
				"	  PD.COD_PRODUCTO," +     
				"	  PC.NOMBRE NOMBRE_PRODUCTO," + 
				"	  me.COD_MODELO," + 
				"	  me.NOMBRE modelo," + 
				"	  TV.VALOR modeloNC," +
				"	  EI.NUMERO_SERIE" + 
				"	FROM" +    
				"	  TELEDUCTOS.SERVICIO S," +     
				"	  comun.PRODUCTO_DETALLE pd," +     
				"	  comun.PRODUCTO_CLIENTE pc," + 
				"	  teleductos.SERVICIO_EQUIPO se," + 
				"	  teleductos.MODELO_EQUIPO me," + 
				"	  teleductos.EQUIPO_INSTALACION ei," + 
				"	  COMUN.TABLA_VALORES TV" + 
				"	WHERE" +     
				"	      pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE" +     
				"	  AND S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA')" +     
				"	  AND S.FEC_FIN_VIGENCIA > SYSDATE" +     
				"	  AND Pd.COD_PRODUCTO = pc.COD_PRODUCTO" +     
				"	  AND pc.GENERA_FLUJO_EQUIPOS = 'SI'" + 
				"	  AND se.COD_EQUIPO = ei.COD_EQUIPO" + 
				"	  AND ei.COD_MODELO = me.COD_MODELO" + 
				"	  AND se.COD_SERVICIO = s.cod_servicio" + 
				"	  AND TV.NOMBRE_TABLA = 'TRADUCCION_EQUIPOS'" +
				"	  AND tv.COD_VALOR = me.nombre";

			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {
				
				ServicioEquipoDTO servicio = new ServicioEquipoDTO();
				
				servicio.setCodServicio(rs.getString("COD_SERVICIO"));
				servicio.setCodProducto(rs.getString("COD_PRODUCTO"));
				servicio.setNombreProducto(rs.getString("NOMBRE_PRODUCTO"));
				servicio.setModelo(rs.getString("modelo"));				
				servicio.setCodServicioEquipo(rs.getString("COD_SERVICIO"));
				servicio.setCodModelo(rs.getString("COD_MODELO"));
				servicio.setModeloNC(rs.getString("modeloNC"));
				servicio.setNumeroSerie(rs.getString("NUMERO_SERIE"));
				
				lServicios.add(servicio);				
			}
			
			rs.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return lServicios;
	}

	private static void limpiaTablaEquipos(Connection conn) {
		
		System.out.println("Limpia Tabla");
		
		try {
			
			String sql = "delete from teleductos.servicio_migracion_equipo2" ;		

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}				
	}

	private static Connection abreConexion(String usuario) {
		
		Connection conn = null;
		
		try {
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			  conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.7:1531/expgtd.grupogtd.com", usuario,"kronner");// PRoduccion
			  //conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.10:1531/expgtd.grupogtd.com", usuario,"massu");
	
			  conn.setAutoCommit(false);
			
		}  catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	
		return conn;
	}
	
	private static void buscarModeloSGE(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =

				"SELECT"
				+"    se.COD_SERVICIO, me.COD_MODELO, me.NOMBRE modelo"
				+" FROM"
				+"    teleductos.SERVICIO_EQUIPO se,"
				+"    teleductos.MODELO_EQUIPO me,"
				+"    teleductos.EQUIPO_INSTALACION ei"
				+" WHERE"
				+"    se.COD_EQUIPO = ei.COD_EQUIPO"
				+"    AND ei.COD_MODELO = me.COD_MODELO"
				+"       AND se.COD_SERVICIO = " + equipo.getCodServicioEquipo();
			
			PreparedStatement stmt = conn.prepareStatement(sql);		
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				equipo.setModelo(rs.getString("modelo"));				
				equipo.setCodServicioEquipo(rs.getString("COD_SERVICIO"));
				equipo.setCodModelo(rs.getString("COD_MODELO"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarModeloNC(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =

				"SELECT TV.VALOR " +
				"FROM COMUN.TABLA_VALORES TV " +
				"WHERE TV.NOMBRE_TABLA = 'TRADUCCION_EQUIPOS' " +
				"AND tv.COD_VALOR = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, equipo.getModelo());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {	
				equipo.setModeloNC(rs.getString("VALOR"));		
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDireccionEquipo(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				" SELECT s.cod_direccion_origen COD_ORIGEN, mdOrigen.direccion ORIGEN" + 
				" FROM teleductos.servicio s, COMUN.mae_direcciones mdOrigen" + 
				" WHERE s.COD_SERVICIO = ?" + 
				" and s.cod_direccion_origen = mdOrigen.cod_direccion";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodServicioEquipo());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setCodDireccionEquipo(rs.getString("COD_ORIGEN"));
				equipo.setDireccionEquipo(rs.getString("ORIGEN"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDireccionDestinoEquipo(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				" SELECT s.cod_direccion_destino COD_DESTINO, mdDestino.direccion DESTINO" + 
				" FROM teleductos.servicio s, COMUN.mae_direcciones mdDestino" + 
				" WHERE s.COD_SERVICIO = ?" + 
				" and s.cod_direccion_destino = mdDestino.cod_direccion";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodServicioEquipo());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setCodDireccionEquipo(rs.getString("COD_DESTINO"));
				equipo.setDireccionEquipo(rs.getString("DESTINO"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarEnlaceAsociadoAEquipo(Connection conn, String codServicio, ServicioEquipoDTO servicio) {
		
		try {
			String sql =
				" select sm.cod_servicio_migrado, sm.migracion_id" +  
				" from teleductos.servicio_migracion2 sm" +  
				" where sm.migracion_id in ('10000045','10001193','10001255','10001255')" + 
				" and sm.cod_servicio = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, codServicio);
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				servicio.setCodServicioMigrado(rs.getString("cod_servicio_migrado"));
				servicio.setMigracionIdServicioMigrado(rs.getString("migracion_id"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarEnlacesAsociadosNuevos(Connection conn, String codServicioEnlace, String codServicioMigrado, ServicioEquipoDTO servicio) {
		
		try {
			
			String productosAsociados = "";
			
			String sql =
				" select sm.cod_servicio_migrado" +  
				" from teleductos.servicio_migracion2 sm" +  
				" where sm.cod_servicio_migrado <> ?" + 
				" and sm.cod_servicio = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, codServicioMigrado);
			stmt.setString(2, codServicioEnlace);
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				productosAsociados = productosAsociados + rs.getString("cod_servicio_migrado") + ";";
			}
			
			System.out.println("Associated Products: " + productosAsociados);
			
			if(productosAsociados.length() > 0){
				servicio.setAssociatedProduct(productosAsociados.substring(0, (productosAsociados.length()-1)));
			} else {
				servicio.setAssociatedProduct(null);
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDatosMigracion_ModeloEquipos(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"select pm.migration_id, pm.offering, pm.product_code from comun.poc_migracion pm " +
				"where pm.poc_familia = 'Equipamiento' and pm.cod_producto = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodModelo());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setMigracionId(rs.getString("migration_id"));
				equipo.setOffering(rs.getString("offering"));
				equipo.setProductCode(rs.getString("product_code"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDatosMigracion_ModeloEquipos_Standalone(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"select pm.migration_id, pm.offering, pm.product_code from comun.poc_migracion pm " +
				"where pm.poc_familia = 'Equipamiento_Standalone' and pm.cod_producto = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodModelo());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setMigracionId(rs.getString("migration_id"));
				equipo.setOffering(rs.getString("offering"));
				equipo.setProductCode(rs.getString("product_code"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDatosMigracion_CaractValor(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"	select" +  
				"	    pm.migration_id, pm.offering, pm.product_code" +  
				"	from teleductos.servicio s," + 
				"	    comun.producto_valor pv," + 
				"	    comun.caracteristica_valor cv," + 
				"	    comun.poc_migracion pm" + 
				"	where S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA')" +   
				"	AND S.FEC_FIN_VIGENCIA > SYSDATE" + 
				"	  AND S.FEC_FIN_VIGENCIA = (SELECT MAX(S2.FEC_FIN_VIGENCIA) FROM TELEDUCTOS.SERVICIO S2 WHERE S2.COD_SERVICIO = S.COD_SERVICIO)" + 
				"	  AND pv.cod_producto = s.COD_PRODUCTO_CLIENTE" + 
				"	  AND cv.cod_valor = pv.cod_valor" + 
				/*"	  AND cv.nombre in (" + 
				"	    'Cisco 861', 'Cisco 881', 'Cisco 2921', 'CISCO 861-K9', 'CISCO 891-K9', 'CISCO 2901/K9', 'CISCO 2921/K9', 'WS-C2960-24PC-S', 'WS-C2960-48PST-S'," +  
				"	'AIR-CT2504-50-K9', 'EHWIC-4ESG', 'CISCO2901-SEC/K9', 'CISCO2921-SEC/K9', 'WS-C2960-24PC-S', 'CISCO891/K9', 'AIR-CT2504-15-K9'," +  
				"	'AIR-CT2504-25-K9', 'AIR-CT2504-5-K9', 'WS-C2960-8TC-S', 'WS-C2960-48TC-S', 'CISCO2901-SEC/K9', 'CISCO2921-SEC/K9', 'WS-C2960-48PST-S'," +  
				"	'WS-C2960-8TC-S ', 'WS-C2960-48TC-S', 'CISCO 881-V-K9', 'CISCO 1811', 'CISCO 2811', 'C3925-SEC/K9', 'FORTINET FG-500D', 'FORTINET FG-500D'," +  
				"	'FORTINET FG-300D', 'C891F-K9', 'CISCO3925-SEC/K9', 'WS-C2960C-8TC-S', 'WS-C2960+48TC-S', 'C881-k9 + PoE', 'C891F-K9', 'C891-k9 + PoE'," +  
				"	'3850-24-S-E', 'WS-C2960X-24TS-LL', 'WS-C2960X-24PS-L', 'WS-C2960C-12PC-L  (CMP-DIN-MN)', 'WS-C3650-24TS-E', 'WS-C3650-24TS-S'," +  
				"	'WS-C3650-24TD-E', 'ISR4451-X-Sec/K9', 'Meraki MR33-HW', 'WS-C2960X-24TS-LL', 'Cisco 3945-HSEC/K9', 'WS-C2960x-48TD-L')" + */
				"	  AND pm.cod_producto = cv.cod_valor" + 
				"	  AND s.COD_SERVICIO = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodServicio());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setMigracionId(rs.getString("migration_id"));
				equipo.setOffering(rs.getString("offering"));
				equipo.setProductCode(rs.getString("product_code"));
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void buscarDireccionEnlace(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				" SELECT s.cod_direccion_origen COD_ORIGEN, mdOrigen.direccion ORIGEN, s.cod_direccion_destino COD_DESTINO, mdDestino.direccion DESTINO" + 
				" FROM teleductos.servicio s, COMUN.mae_direcciones mdOrigen, COMUN.mae_direcciones mdDestino" + 
				" WHERE s.COD_SERVICIO = ?" +  
				" and s.cod_direccion_origen = mdOrigen.cod_direccion" + 
				" and s.cod_direccion_destino = mdDestino.cod_direccion";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, equipo.getCodServicio());
			
			ResultSet rs = stmt.executeQuery();			
	
			while (rs.next()) {
				
				equipo.setCodDireccionEnlaceOrigen(rs.getString("COD_ORIGEN"));
				equipo.setDireccionEnlaceOrigen(rs.getString("ORIGEN"));
				equipo.setCodDireccionEnlaceDestino(rs.getString("COD_DESTINO"));
				equipo.setDireccionEnlaceDestino(rs.getString("DESTINO"));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static boolean esServicioPrincipal(String codServicio, String codServicioRelacion, 
												Connection conn) {
		
		boolean esServicioPrincipal = false;
		
		try {
			String sql =
				" SELECT" + 
				"       sr.COD_SERVICIO" + 
				" FROM" +   
				"	   teleductos.SERVICIO_RELACION sr" + 
				" WHERE" +   
				"       sr.TIPO_RELACION = 'RES'" + 
				"	    AND sr.COD_SERVICIO = ?" + 
				"       AND sr.COD_SERVICIO_RELACION = ?";

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, codServicio);
			stmt.setString(2, codServicioRelacion);

			ResultSet rs = stmt.executeQuery();
	
			if (rs.next()) {
				esServicioPrincipal = true;
			}
			
			rs.close();
			rs = null;
			
			stmt.close();
			stmt = null;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} //codigo servicio
		
		return esServicioPrincipal;
	}
}
