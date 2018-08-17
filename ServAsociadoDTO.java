package procesoMigracion.dto;

public class ServAsociadoDTO {
	
	private String codServicio;
	private String codDireccionEnlaceOrigen;
	private String codDireccionEnlaceDestino;
	
	/**
	 * 
	 */
	public ServAsociadoDTO() {
		super();
	}
	/**
	 * @return the codServicio
	 */
	public String getCodServicio() {
		return codServicio;
	}
	/**
	 * @param codServicio the codServicio to set
	 */
	public void setCodServicio(String codServicio) {
		this.codServicio = codServicio;
	}
	/**
	 * @return the codDireccionEnlaceOrigen
	 */
	public String getCodDireccionEnlaceOrigen() {
		return codDireccionEnlaceOrigen;
	}
	/**
	 * @param codDireccionEnlaceOrigen the codDireccionEnlaceOrigen to set
	 */
	public void setCodDireccionEnlaceOrigen(String codDireccionEnlaceOrigen) {
		this.codDireccionEnlaceOrigen = codDireccionEnlaceOrigen;
	}
	/**
	 * @return the codDireccionEnlaceDestino
	 */
	public String getCodDireccionEnlaceDestino() {
		return codDireccionEnlaceDestino;
	}
	/**
	 * @param codDireccionEnlaceDestino the codDireccionEnlaceDestino to set
	 */
	public void setCodDireccionEnlaceDestino(String codDireccionEnlaceDestino) {
		this.codDireccionEnlaceDestino = codDireccionEnlaceDestino;
	}
}
