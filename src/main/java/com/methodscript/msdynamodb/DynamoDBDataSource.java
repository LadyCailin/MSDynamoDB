/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.methodscript.msdynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.laytonsmith.PureUtilities.Common.StringUtils;
import com.laytonsmith.PureUtilities.DaemonManager;
import com.laytonsmith.PureUtilities.Version;
import com.laytonsmith.PureUtilities.Web.WebUtility;
import com.laytonsmith.annotations.datasource;
import com.laytonsmith.core.MSVersion;
import com.laytonsmith.persistence.AbstractDataSource;
import com.laytonsmith.persistence.DataSource;
import com.laytonsmith.persistence.DataSourceException;
import com.laytonsmith.persistence.ReadOnlyException;
import com.laytonsmith.persistence.io.ConnectionMixinFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@datasource("dynamodb")
public class DynamoDBDataSource extends AbstractDataSource {

	private static final String PRIMARY_KEY_NAME = "primary";

	private String protocol = "http";
	private String host;
	private int port = 8000;
	private Regions region = Regions.US_EAST_1;
	private String accessKeyId = null;
	private String accessKeySecret = null;
	private AmazonDynamoDB client;
	private String tableName;

	private DynamoDBDataSource() {

	}

	@SuppressWarnings("UseSpecificCatch")
	public DynamoDBDataSource(URI uri, ConnectionMixinFactory.ConnectionMixinOptions options)
			throws DataSourceException {
		super(uri, options);
		Map<String, String> queryString = WebUtility.getQueryMap(uri.getQuery());
		if(queryString.containsKey("protocol")) {
			protocol = queryString.get("protocol");
		}
		host = uri.getHost();
		if("aws".equalsIgnoreCase(host)) {
			host = null;
		}
		port = uri.getPort();
		if(queryString.containsKey("region")) {
			region = Regions.fromName(queryString.get("region"));
		}
		if(queryString.containsKey("accessKeyId") && queryString.containsKey("accessKeySecret")) {
			accessKeyId = queryString.get("accessKeyId");
			accessKeySecret = queryString.get("accessKeySecret");
		}
		if(!queryString.containsKey("tableName")) {
			throw new DataSourceException("tableName is a required parameter in the DynamoDB configuration.");
		}
		tableName = queryString.get("tableName");
		AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard();
		if(host != null) {
			clientBuilder.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(protocol + "://" + host + ":" + port,
							region.name().toLowerCase()));
		} else {
			clientBuilder.withRegion(region);
		}
		if(accessKeyId != null) {
			clientBuilder.setCredentials(new AWSCredentialsProvider() {
				@Override
				public AWSCredentials getCredentials() {
					return new AWSCredentials() {
						@Override
						public String getAWSAccessKeyId() {
							return accessKeyId;
						}

						@Override
						public String getAWSSecretKey() {
							return accessKeySecret;
						}
					};
				}

				@Override
				public void refresh() {}
			});
		}
		client = clientBuilder.build();
		try {
			client.describeTable(tableName).getTable();
		} catch (ResourceNotFoundException e) {
			// Need to create a table with this name
			if(host == null) {
				throw new DataSourceException("The table \"" + tableName + "\" was not found in "
						+ (host == null ? "AWS:" + region.getName() : host)
						+ ". You must manually create this table yourself. Please see the documentation for details"
						+ " on how to set this up.");
			} else {
				createTable(tableName, client);
			}
		}
	}

	private static void createTable(String tableName, AmazonDynamoDB client) {
		// THIS SHOULD ONLY BE USED LOCALLY
		client.createTable(new CreateTableRequest(tableName,
			Arrays.asList(new KeySchemaElement(PRIMARY_KEY_NAME, KeyType.HASH).withAttributeName(PRIMARY_KEY_NAME)))
				.withAttributeDefinitions(new AttributeDefinition(PRIMARY_KEY_NAME, ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput(1000L, 1000L)));
	}

	@Override
	protected void startTransaction0(DaemonManager dm) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected void stopTransaction0(DaemonManager dm, boolean rollback) throws DataSourceException, IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected boolean set0(DaemonManager dm, String[] key, String value) throws ReadOnlyException,
			DataSourceException, IOException {
		Map<String, AttributeValue> map = new HashMap<>();
		map.put(PRIMARY_KEY_NAME, new AttributeValue(StringUtils.Join(key, ".")).withS(value));
		client.putItem(new PutItemRequest(tableName, map));
		return true;
	}

	@Override
	protected String get0(String[] key) throws DataSourceException {
		Map<String, AttributeValue> map = new HashMap<>();
		map.put(PRIMARY_KEY_NAME, new AttributeValue(StringUtils.Join(key, ".")));
		Map<String, AttributeValue> ret = client.getItem(new GetItemRequest(tableName, map)).getItem();
		if(ret == null) {
			return null;
		} else {
			return ret.get(StringUtils.Join(key, ".")).getS();
		}
	}

	@Override
	public Set<String[]> keySet(String[] keyBase) throws DataSourceException {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void populate() throws DataSourceException {

	}

	@Override
	public EnumSet<DataSource.DataSourceModifier> implicitModifiers() {
		return null;
	}

	@Override
	public EnumSet<DataSource.DataSourceModifier> invalidModifiers() {
		return null;
	}

	@Override
	public void disconnect() throws DataSourceException {

	}

	@Override
	public String docs() {
		return "TODO";
	}

	@Override
	public Version since() {
		return MSVersion.V3_3_4;
	}

}
