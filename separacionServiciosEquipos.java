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

			for (ServicioEquipoDTO servicio : lServicios) {
				
				obtieneModeloEquipos(conn, servicio);
				
				if(servicio.getModelo() == null || servicio.getModelo().equals("")){
					obtieneModelo_DatosMaestro(conn, servicio);
				}
				
				if(servicio.getModeloNC() != null && !servicio.getModeloNC().equals("")){
					
					//Se obtienen todos los equipos con sus enlaces asociados.
					List<ServAsociadoDTO> serviciosAsociados = getServiciosAsociadosEnlace(servicio,conn);
					
					buscarDatosMigracion_Equipos(conn, serviciosAsociados.size()<= 0? "Equipamiento_Standalone":"Equipamiento", servicio);
					if(servicio.getMigracionId() == null){
						buscarDatosMigracion_CaractValor(conn, servicio);
					}

					if (serviciosAsociados.size()<= 0) {
						
						insertServicioMigracionEquipo(servicio,null,servicio.getModelo(),servicio.getModeloNC(),
								servicio.getMigracionId(),servicio.getCodProducto(),"ERROR","EQUIPO SIN ENLACE ASOCIADO",
								servicio.getCodDireccionEnlaceDestino(),servicio.getDireccionEnlaceDestino(),conn);
						
					} else {
						
						String clienteEnlace = getCliente(serviciosAsociados.get(0).getCodServicio(), conn);
						String clienteEquipo = getCliente(servicio.getCodServicioEquipo(), conn);
						
						buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(0).getCodServicio(), servicio);
						buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(0).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
								
						if(servicio.getProductCode() != null && (servicio.getProductCode().equals("SWTCHL2") || servicio.getProductCode().equals("SWTCHL3"))){
							
							if(serviciosAsociados.size() == 1){
								
								if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
							
									if(servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(0).getCodDireccionEnlaceOrigen()) ||
											servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(0).getCodDireccionEnlaceDestino())){
									
										insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", 
											servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
									
									} else {
										
										//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE
										insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
												servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
												"ERROR","EQUIPO CON DIRECCION DE ORIGEN DIFERENTE A LA DEL ENLACE",
												servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
									}
									
								} else {
									
									//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
									insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
											"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",
											servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
								}
								
							} else if(serviciosAsociados.size() == 2){
								
								if(esServicioPrincipal(serviciosAsociados.get(0).getCodServicio(), 
										serviciosAsociados.get(1).getCodServicio(), conn)){
									
									if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
									
										if(servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(0).getCodDireccionEnlaceOrigen()) ||
												servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(0).getCodDireccionEnlaceDestino())){
											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", 
													servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
											
										} else {
											
											//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
													"ERROR","EQUIPO CON DIRECCION DE ORIGEN DIFERENTE A LA DEL ENLACE",
													servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
										}
										
									} else {
										
										//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
										insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
												servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
												"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",
												servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
									}
								
								} else if(esServicioPrincipal(serviciosAsociados.get(1).getCodServicio(), 
										serviciosAsociados.get(0).getCodServicio(), conn)){
									
									clienteEnlace = getCliente(serviciosAsociados.get(1).getCodServicio(), conn);

									buscarEnlaceAsociadoAEquipo(conn, serviciosAsociados.get(1).getCodServicio(), servicio);
									buscarEnlacesAsociadosNuevos(conn, serviciosAsociados.get(1).getCodServicio(), servicio.getCodServicioMigrado(), servicio);
									
									if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
										
										if(servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(1).getCodDireccionEnlaceOrigen()) ||
												servicio.getCodDireccionEnlaceOrigen().equals(serviciosAsociados.get(1).getCodDireccionEnlaceDestino())){
											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", 
													servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
											
										} else {
											
											//EQUIPO CON DIRECCION DIFERENTE A LA DEL ENLACE											
											insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
													servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
													"ERROR","EQUIPO CON DIRECCION DE ORIGEN DIFERENTE A LA DEL ENLACE",
													servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
										}
									
									} else {
									
										//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE
										insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
												servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
												"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",
												servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
									}
									
								} else {
									
									//SERVICIOS ASOCIADOS NO SON UN SERVICIO PRINCIPAL ENTRE ELLOS									
									insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
											"ERROR","SERVICIOS ASOCIADOS NO SON UN SERVICIO PRINCIPAL ENTRE ELLOS",
											servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
								}
								
							} else if(serviciosAsociados.size() > 2){
								
								//migrar Switch como offering “Equipamiento” (familia Otros de NC). PENDIENTE IT7
								//EQUIPO CON MAS DE 2 SERVICIOS ASOCIADOS. (PENDIENTE IT7) 								
								insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
										servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
										"ERROR","EQUIPO CON MAS DE 2 SERVICIOS ASOCIADOS. (PENDIENTE IT7)",
										servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
							}
							
						} else if(servicio.getProductCode() != null && (servicio.getProductCode().equals("ROUTR") || servicio.getProductCode().equals("RTRFIREW") ||
								servicio.getProductCode().equals("RTR_GW"))){
							
							if(serviciosAsociados.size() == 1){
								
								if (clienteEnlace != null && clienteEquipo != null && clienteEnlace.equals(clienteEquipo)) {
								
									insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),"OK","OK", 
											servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
									
								} else {
									
									//EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE									
									insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
											servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
											"ERROR","EQUIPO CON CLIENTE DIFERENTE AL DEL ENLACE",
											servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
								}
								
							} else if(serviciosAsociados.size() > 1){
								
								//migrar Switch como offering “Equipamiento” (familia Otros de NC). PENDIENTE IT7
								//EQUIPO CON MAS DE 1 SERVICIO ASOCIADO. (PENDIENTE IT7) 
								insertServicioMigracionEquipo(servicio,servicio.getCodServicioMigrado(),servicio.getModelo(),
										servicio.getModeloNC(),servicio.getMigracionId(),servicio.getCodProducto(),
										"ERROR","EQUIPO CON MAS DE 1 SERVICIO ASOCIADO. (PENDIENTE IT7)",
										servicio.getCodDireccionEnlaceOrigen(), servicio.getDireccionEnlaceOrigen(),conn);
							}
						}	
					}
					
					conn.commit();					
				}
			}
			
			conn.close();
			conn = null;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("Fin del proceso");		
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
	
	private static void insertServicioMigracionEquipo(ServicioEquipoDTO servicio, String codServicioMigrado, String  modeloSge, String modeloNC,
			String migrationId, String codProducto, String estado, String descripcion, 
			String codDireccion, String direccion, Connection conn) {
		
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
				"	  s.cod_direccion_destino COD_DESTINO," +
				"	  mdDestino.direccion DESTINO," +
				"	  s.cod_direccion_origen COD_ORIGEN," +
				"	  mdOrigen.direccion ORIGEN," + 
				"	  PD.COD_PRODUCTO," +     
				"	  PC.NOMBRE NOMBRE_PRODUCTO" + 
				"	FROM" +    
				"	  TELEDUCTOS.SERVICIO S," + 
				"	  COMUN.MAE_DIRECCIONES mdDestino," +
				"	  COMUN.MAE_DIRECCIONES mdOrigen," + 
				"	  comun.PRODUCTO_DETALLE pd," +     
				"	  comun.PRODUCTO_CLIENTE pc" + 
				"	WHERE" +     
				"	      pd.COD_PRODUCTO_DETALLE = s.COD_PRODUCTO_CLIENTE" +     
				"	  AND S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA')" +     
				"	  AND S.FEC_FIN_VIGENCIA > SYSDATE" + 
				"	  AND S.FEC_FIN_VIGENCIA = (SELECT MAX(S2.FEC_FIN_VIGENCIA) FROM TELEDUCTOS.SERVICIO S2 WHERE S2.COD_SERVICIO = S.COD_SERVICIO)" +
				"	  AND s.cod_direccion_destino = mdDestino.cod_direccion" +
				"	  AND s.cod_direccion_origen = mdOrigen.cod_direccion" +
				"	  AND Pd.COD_PRODUCTO = pc.COD_PRODUCTO" +     
				"	  AND pc.GENERA_FLUJO_EQUIPOS = 'SI'";

			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {
				
				ServicioEquipoDTO servicio = new ServicioEquipoDTO();
				
				servicio.setCodServicio(rs.getString("COD_SERVICIO"));
				servicio.setCodDireccionEnlaceDestino(rs.getString("COD_DESTINO"));
				servicio.setDireccionEnlaceDestino(rs.getString("DESTINO"));
				servicio.setCodDireccionEnlaceOrigen(rs.getString("COD_ORIGEN"));
				servicio.setDireccionEnlaceOrigen(rs.getString("ORIGEN"));
				servicio.setCodProducto(rs.getString("COD_PRODUCTO"));
				servicio.setNombreProducto(rs.getString("NOMBRE_PRODUCTO"));				
				servicio.setCodServicioEquipo(rs.getString("COD_SERVICIO"));
				
				lServicios.add(servicio);				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return lServicios;
	}
	
	private static void obtieneModeloEquipos(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"SELECT " + 
				"  me.COD_MODELO, " +   
				"  me.NOMBRE modelo, " +   
				"  TV.VALOR modeloNC, " + 
				"  EI.NUMERO_SERIE " +   
				" FROM " + 
				"  TELEDUCTOS.SERVICIO S, " + 
				"  teleductos.SERVICIO_EQUIPO se, " + 
				"  teleductos.MODELO_EQUIPO me, " + 
				"  teleductos.EQUIPO_INSTALACION ei, " +   
				"  COMUN.TABLA_VALORES TV " +   
				" WHERE " + 
				"	S.COD_SERVICIO = ? " + 
				"  AND S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA') " +       
				"  AND S.FEC_FIN_VIGENCIA > SYSDATE " + 
				"  AND S.FEC_FIN_VIGENCIA = (SELECT MAX(S2.FEC_FIN_VIGENCIA) FROM TELEDUCTOS.SERVICIO S2 WHERE S2.COD_SERVICIO = S.COD_SERVICIO) " +  
				"  AND se.COD_SERVICIO = s.cod_servicio " + 
				"  AND se.COD_EQUIPO = ei.COD_EQUIPO " + 
				"  AND ei.COD_MODELO = me.COD_MODELO " + 
				"  AND TV.NOMBRE_TABLA = 'TRADUCCION_EQUIPOS' " +  
				"  AND tv.COD_VALOR = me.nombre";

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, equipo.getCodServicio());
			
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {				
				equipo.setModelo(rs.getString("modelo"));
				equipo.setCodModelo(rs.getString("COD_MODELO"));
				equipo.setModeloNC(rs.getString("modeloNC"));
				equipo.setNumeroSerie(rs.getString("NUMERO_SERIE"));				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	private static void obtieneModelo_DatosMaestro(Connection conn, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"select " +    
				"	cv.cod_valor cod_modelo, " +
				"	cv.nombre modelo, " +
				"	TV.VALOR modeloNC " +    
				" from teleductos.servicio s, " +   
				"	comun.producto_valor pv, " +   
				"	comun.caracteristica_valor cv, " + 
				"    COMUN.TABLA_VALORES TV " + 
				" where S.ESTADO_SERVICIO NOT IN ('ANULA','RETIRA') " +     
				"  AND S.FEC_FIN_VIGENCIA > SYSDATE " +   
				"  AND S.FEC_FIN_VIGENCIA = (SELECT MAX(S2.FEC_FIN_VIGENCIA) FROM TELEDUCTOS.SERVICIO S2 WHERE S2.COD_SERVICIO = S.COD_SERVICIO) " +   
				"  AND pv.cod_producto = s.COD_PRODUCTO_CLIENTE " +   
				"  AND cv.cod_valor = pv.cod_valor " +   
				"  AND cv.cod_caracteristica in (424,1838,1939,418,2018,2019,2118,2119,2120," +
				"  2278,2279,2280,2498,1841,2098,1839,2599,1492,1504,1759,1498,1758,420,422,2798,3038) " + 
				"  AND TV.NOMBRE_TABLA = 'TRADUCCION_EQUIPOS' " +   
				"  AND tv.COD_VALOR = cv.nombre " + 
				"  AND s.COD_SERVICIO = ?";

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, equipo.getCodServicio());
			
			ResultSet rs = stmt.executeQuery();
	
			while (rs.next()) {				
				equipo.setModelo(rs.getString("modelo"));
				equipo.setCodModelo(rs.getString("COD_MODELO"));
				equipo.setModeloNC(rs.getString("modeloNC"));				
			}
			
			stmt.close();
			stmt = null;
			
			rs.close();
			rs = null;

		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}

	private static void limpiaTablaEquipos(Connection conn) {
		
		System.out.println("Limpia Tabla");
		
		try {
			
			String sql = "delete from teleductos.servicio_migracion_equipo2" ;		

			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			
			stmt.close();
			stmt = null;
			
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
	
	private static void buscarEnlaceAsociadoAEquipo(Connection conn, String codServicio, ServicioEquipoDTO servicio) {
		
		try {
			String sql =
				" select sm.cod_servicio_migrado, sm.migracion_id" +  
				" from teleductos.servicio_migracion2 sm" +  
				" where sm.migracion_id in ('10000045','10001193','10001255')" + 
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
	
	private static void buscarDatosMigracion_Equipos(Connection conn, String familiaPoc, ServicioEquipoDTO equipo) {
		
		try {
			String sql =
				"select pm.migration_id, pm.offering, pm.product_code from comun.poc_migracion pm " +
				"where pm.poc_familia = ? and pm.producto = ?";
			
			PreparedStatement stmt = conn.prepareStatement(sql);	
			stmt.setString(1, familiaPoc);
			stmt.setString(2, equipo.getModelo());
			
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
				"	  AND pm.producto = cv.nombre" + 
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
