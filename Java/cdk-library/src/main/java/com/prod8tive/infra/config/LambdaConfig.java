package com.prod8ctive.infra.config;

import java.util.LinkedList;
import java.util.List;

public class LambdaConfig {

    String confluentLayerName;
    String confluentLayerArn;

    String pythonLayerName;
    String pythonLayerArn;

    String lambdaRoleArn;

    String securityGroupId;

    String vpcId;
    LinkedList<String> privateSubnetIds;
    List<String> availabilityZones;

    List<String> eventSourceTriggerVpcSubnets;
    List<String> kafkaBootstrapServerList;
    String kafkaConnectionSecretName;
    String kafkaConnectionSecretArn;
    String schemaRegistryUrl;
    String schemaRegistryConnectionSecretName;
    String schemaRegistryConnectionSecretArn;
    String lambdaFilePath;
    String lambdaPolicyDocumentFilePath;

    public String getConfluentLayerName() {
        return confluentLayerName;
    }

    public void setConfluentLayerName(String confluentLayerName) {
        this.confluentLayerName = confluentLayerName;
    }

    public String getConfluentLayerArn() {
        return confluentLayerArn;
    }

    public void setConfluentLayerArn(String confluentLayerArn) {
        this.confluentLayerArn = confluentLayerArn;
    }

    public String getPythonLayerName() {
        return pythonLayerName;
    }

    public void setPythonLayerName(String pythonLayerName) {
        this.pythonLayerName = pythonLayerName;
    }

    public String getPythonLayerArn() {
        return pythonLayerArn;
    }

    public void setPythonLayerArn(String pythonLayerArn) {
        this.pythonLayerArn = pythonLayerArn;
    }

    public String getLambdaRoleArn() {
        return lambdaRoleArn;
    }

    public void setLambdaRoleArn(String lambdaRoleArn) {
        this.lambdaRoleArn = lambdaRoleArn;
    }

    public String getSecurityGroupId() {
        return securityGroupId;
    }

    public void setSecurityGroupId(String securityGroupId) {
        this.securityGroupId = securityGroupId;
    }

    public String getVpcId() {
        return vpcId;
    }

    public void setVpcId(String vpcId) {
        this.vpcId = vpcId;
    }

    public LinkedList<String> getPrivateSubnetIds() {
        return privateSubnetIds;
    }

    public void setPrivateSubnetIds(LinkedList<String> privateSubnetIds) {
        this.privateSubnetIds = privateSubnetIds;
    }

    public List<String> getAvailabilityZones() {
        return availabilityZones;
    }

    public void setAvailabilityZones(List<String> availabilityZones) {
        this.availabilityZones = availabilityZones;
    }

    public List<String> getEventSourceTriggerVpcSubnets() {
        return eventSourceTriggerVpcSubnets;
    }

    public void setEventSourceTriggerVpcSubnets(List<String> eventSourceTriggerVpcSubnets) {
        this.eventSourceTriggerVpcSubnets = eventSourceTriggerVpcSubnets;
    }

    public List<String> getKafkaBootstrapServerList() {
        return kafkaBootstrapServerList;
    }

    public void setKafkaBootstrapServerList(List<String> kafkaBootstrapServerList) {
        this.kafkaBootstrapServerList = kafkaBootstrapServerList;
    }

    public String getKafkaConnectionSecretName() {
        return kafkaConnectionSecretName;
    }

    public void setKafkaConnectionSecretName(String kafkaConnectionSecretName) {
        this.kafkaConnectionSecretName = kafkaConnectionSecretName;
    }

    public String getKafkaConnectionSecretArn() {
        return kafkaConnectionSecretArn;
    }

    public void setKafkaConnectionSecretArn(String kafkaConnectionSecretArn) {
        this.kafkaConnectionSecretArn = kafkaConnectionSecretArn;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getSchemaRegistryConnectionSecretName() {
        return schemaRegistryConnectionSecretName;
    }

    public void setSchemaRegistryConnectionSecretName(String schemaRegistryConnectionSecretName) {
        this.schemaRegistryConnectionSecretName = schemaRegistryConnectionSecretName;
    }

    public String getSchemaRegistryConnectionSecretArn() {
        return schemaRegistryConnectionSecretArn;
    }

    public void setSchemaRegistryConnectionSecretArn(String schemaRegistryConnectionSecretArn) {
        this.schemaRegistryConnectionSecretArn = schemaRegistryConnectionSecretArn;
    }

    public String getLambdaFilePath() {
        return lambdaFilePath;
    }

    public void setLambdaFilePath(String lambdaFilePath) {
        this.lambdaFilePath = lambdaFilePath;
    }

    public String getLambdaPolicyDocumentFilePath() {
        return lambdaPolicyDocumentFilePath;
    }

    public void setLambdaPolicyDocumentFilePath(String lambdaPolicyDocumentFilePath) {
        this.lambdaPolicyDocumentFilePath = lambdaPolicyDocumentFilePath;
    }
}
