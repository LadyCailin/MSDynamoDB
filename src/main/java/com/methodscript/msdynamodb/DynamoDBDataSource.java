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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@datasource("dynamodb")
public class DynamoDBDataSource extends AbstractDataSource {

	private static final String PRIMARY_KEY_NAME = "key";
	private static final String VALUE_KEY_NAME = "value";

	private String protocol = "http";
	private String host;
	private int port = 8000;
	private Regions region = Regions.US_EAST_1;
	private String accessKeyId = null;
	private String accessKeySecret = null;
	private String tableName;
	private AmazonDynamoDB client;
	private Table table;

	private DynamoDBDataSource() {

	}

	@SuppressWarnings("UseSpecificCatch")
	public DynamoDBDataSource(URI uri, ConnectionMixinFactory.ConnectionMixinOptions options)
			throws DataSourceException {
		super(uri, options);
		Map<String, String> queryString = WebUtility.getQueryMap(uri.getQuery());
		if (queryString.containsKey("protocol")) {
			protocol = queryString.get("protocol");
		}
		host = uri.getHost();
		if ("aws".equalsIgnoreCase(host)) {
			host = null;
		}
		port = uri.getPort();
		if (queryString.containsKey("region")) {
			region = Regions.fromName(queryString.get("region"));
		}
		if (queryString.containsKey("accessKeyId") && queryString.containsKey("accessKeySecret")) {
			accessKeyId = queryString.get("accessKeyId");
			accessKeySecret = queryString.get("accessKeySecret");
		}
		if (!queryString.containsKey("tableName")) {
			throw new DataSourceException("tableName is a required parameter in the DynamoDB configuration.");
		}
		tableName = queryString.get("tableName");
		validateTableName(tableName);
		AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard();
		if (host != null) {
			clientBuilder.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(protocol + "://" + host + ":" + port,
							region.name().toLowerCase()));
		} else {
			clientBuilder.withRegion(region);
		}
		if (accessKeyId != null) {
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
				public void refresh() {
				}
			});
		}
		client = clientBuilder.build();
		try {
			client.describeTable(tableName).getTable();
		} catch (ResourceNotFoundException e) {
			// Need to create a table with this name
			if (host == null) {
				throw new DataSourceException("The table \"" + tableName + "\" was not found in "
						+ (host == null ? "AWS:" + region.getName() : host)
						+ ". You must manually create this table yourself. Please see the documentation for details"
						+ " on how to set this up.");
			} else {
				createTable(tableName, client);
			}
		}
		table = new DynamoDB(client).getTable(tableName);
	}

	private void validateTableName(String tableName) throws DataSourceException {
		List<String> errors = new ArrayList<>();
		if (tableName.length() < 3 || tableName.length() > 255) {
			errors.add("Table name length must be between 3 and 255 characters.");
		}
		if (!tableName.matches("[a-zA-Z0-9_\\-\\.]+")) {
			errors.add("Table names may only contain a-z, A-Z, 0-9, -, ., _");
		}
		if (!errors.isEmpty()) {
			throw new DataSourceException("There " + StringUtils.PluralHelper(errors.size(), "error") + " with the"
					+ " the provided table name \"" + tableName + "\":\n- " + StringUtils.Join(errors, "\n- "));
		}
	}

	private static void createTable(String tableName, AmazonDynamoDB client) {
		// THIS SHOULD ONLY BE USED LOCALLY
		DynamoDB dynamoDB = new DynamoDB(client);

		List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(PRIMARY_KEY_NAME)
				.withAttributeType(ScalarAttributeType.S));

		List<KeySchemaElement> keySchema = new ArrayList<>();
		keySchema.add(new KeySchemaElement().withAttributeName(PRIMARY_KEY_NAME).withKeyType(KeyType.HASH));

		CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName)
				.withKeySchema(keySchema)
				.withAttributeDefinitions(attributeDefinitions)
				.withProvisionedThroughput(new ProvisionedThroughput()
						.withReadCapacityUnits(1000L)
						.withWriteCapacityUnits(1000L));

		Table table = dynamoDB.createTable(request);
		try {
			table.waitForActive();
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void startTransaction0(DaemonManager dm) {
		// TODO: We can create a batch mode with this, but it still has to be paginated, because batches can only
		// be up to 25 items, due to limitations in AWS.
	}

	@Override
	protected void stopTransaction0(DaemonManager dm, boolean rollback) throws DataSourceException, IOException {

	}

	@Override
	protected boolean set0(DaemonManager dm, String[] key, String value) throws ReadOnlyException,
			DataSourceException, IOException {
		Item item = new Item()
				.withPrimaryKey(PRIMARY_KEY_NAME, StringUtils.Join(key, "."))
				.withString(VALUE_KEY_NAME, value);
		table.putItem(item);
		return true;
	}

	@Override
	protected String get0(String[] key) throws DataSourceException {
		Item item = table.getItem(PRIMARY_KEY_NAME, StringUtils.Join(key, "."));
		if (item == null) {
			return null;
		}
		return item.getString(VALUE_KEY_NAME);
	}

	@Override
	public Set<String[]> keySet(String[] keyBase) throws DataSourceException {
		ScanResult res = client.scan(new ScanRequest(tableName));
		Set<String[]> ret = new HashSet<>();
		for (Map<String, AttributeValue> r : res.getItems()) {
			ret.add(r.get(PRIMARY_KEY_NAME).getS().split("\\."));
		}
		return ret;
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
		client = null;
		table = null;
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
