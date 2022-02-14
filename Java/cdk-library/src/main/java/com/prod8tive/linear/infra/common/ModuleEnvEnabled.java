package com.disney.linear.infra.common;

/**
 * 
 * @author chawl001
 */
public interface ModuleEnvEnabled {
	public String getCdkQualifier();
	public String getCdkEnvName();
	public String getModuleName();
	public String getZoneName();
	
	/**
	 * Landing-side Glue Database ID
	 * 
	 * @return 	laap-glue-db-landing-<env> e.g. laap-glue-db-landing-sbx
	 */
	public String getDatabaseIdLanding();
	
	/**
	 * Conformance-side module-specific Glue Database ID
	 * 
	 * @return aap-glue-db-<module-name>-<env> e.g. laap-glue-db-conformance-con-common-sbx
	 */
	public String getDatabaseIdConformance();
}
