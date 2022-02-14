package com.prod8ctive.infra.common;

import static com.prod8ctive.infra.common.CdkConstants.GLUE_DATABASE_PATTERN__LANDING;
import static com.prod8ctive.infra.common.CdkConstants.GLUE_DATABASE_PATTERN__CONFORMANCE;

import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import software.amazon.awscdk.Environment;
import software.amazon.awscdk.IStackSynthesizer;
import software.amazon.awscdk.StackProps;
import software.amazon.awssdk.utils.StringUtils;


/**
 * Module (--context module_name=module-name) and CDK Env (--context env=env-name)
 * 
 * @author chawl001
 */
public interface ModuleEnvEnabledStackProps extends ModuleEnvEnabled, StackProps {
	
	/**
	 * Builder for module and env enabled StackProps
	 * 
	 * @author chawl001
	 * @see software.amazon.awscdk.StackProps.Builder (Thought cannot extend final class StackProps.Builder)
	 */
	public static final class Builder // extends StackProps.Builder 
	{
		private StackProps delegate;
		
		private String cdkQualifier;
		private String cdkEnvName;
		private String cdkModuleName;
		private String cdkZoneName;
		
		public Builder stackProps(StackProps baseStackProps) {
			this.delegate = baseStackProps;
			return this;
		}
		
		public Builder cdkQualifier(@NotNull String qualifier) {
			this.cdkQualifier = qualifier;
			return this;
		}
		
		public Builder cdkEnvName(@NotNull String cdkEnv) {
			this.cdkEnvName = cdkEnv;
			return this;
		}
		
		public Builder moduleName(@NotNull String module) {
			this.cdkModuleName = module;
			return this;
		}
		
		public Builder zoneName(@NotNull String zone) {
			this.cdkZoneName = zone;
			return this;
		}
		
		public ModuleEnvEnabledStackProps build() {
			if (null == this.delegate) {
				throw new RuntimeException("Must provide the baseStackProps");
			}
			if (StringUtils.isBlank(this.cdkEnvName)) {
				throw new RuntimeException("Require property cdkEnvName");
			}
			if (StringUtils.isBlank(this.cdkModuleName)) {
				throw new RuntimeException("Require property moduleName");
			}
			if (StringUtils.isBlank(this.cdkZoneName)) {
				throw new RuntimeException("Require property zoneName");
			}
			
			return new ModuleEnvEnabledStackProps() {
				@Override
				public Environment getEnv() {
//					return ModuleEnvEnabledStackProps.super.getEnv();
					return delegate.getEnv();
				}
				
				@Override
				public String getStackName() {
//					return ModuleEnvEnabledStackProps.super.getStackName();
					return delegate.getStackName();
				}
				
				@Override
				public @Nullable String getDescription() {
//					return ModuleEnvEnabledStackProps.super.getDescription();
					return delegate.getDescription();
				}
				
				@Override
				/**
				 * By default termination protection is on
				 */
				public @Nullable Boolean getTerminationProtection() {
//					Boolean flagObj = ModuleEnvEnabledStackProps.super.getTerminationProtection();
					Boolean flagObj = delegate.getTerminationProtection();
					return (null != flagObj) ? flagObj.booleanValue() : true;
				}
				
				@Override
				public @Nullable IStackSynthesizer getSynthesizer() {
//					return ModuleEnvEnabledStackProps.super.getSynthesizer();
					return delegate.getSynthesizer();
				}
				
				@Override
				public @Nullable Boolean getAnalyticsReporting() {
//					return ModuleEnvEnabledStackProps.super.getAnalyticsReporting();
					return delegate.getAnalyticsReporting();
				}
				
				@Override
				public @Nullable Map<String, String> getTags() {
//					return ModuleEnvEnabledStackProps.super.getTags();
					return delegate.getTags();
				}
				
				@Override
				public String getCdkQualifier() {
					return cdkQualifier;
				}
				
				@Override
				public String getCdkEnvName() {
					return cdkEnvName;
				}
				
				@Override
				public String getModuleName() {
					return cdkModuleName;
				}
				
				@Override 
				public String getZoneName() {
					return cdkZoneName;
				}
				
				@Override
				public String getDatabaseIdLanding() {
					return this.getDatabaseId(true);
				}
				
				@Override
				public String getDatabaseIdConformance() {
					return this.getDatabaseId(false);
				}
				
				private String getDatabaseId(final boolean isLanding) {
					String zoneName = AbstractCdkBuildingBlock.getZoneName(isLanding);
					if (isLanding) {
						String landingGlueDb = String.format(GLUE_DATABASE_PATTERN__LANDING, 
								zoneName, 
								this.getCdkEnvName());
						return landingGlueDb;
					} else {
						String moduleSpecificConformanceGlueDb = String.format(GLUE_DATABASE_PATTERN__CONFORMANCE, 
								zoneName, 
								this.getModuleName(), 
								this.getCdkEnvName());
						return moduleSpecificConformanceGlueDb;
					}
				}
				
			};
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
}
