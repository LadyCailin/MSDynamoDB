/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.methodscript.msdynamodb;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.laytonsmith.PureUtilities.ArgumentParser;
import com.laytonsmith.PureUtilities.ArgumentParser.ArgumentBuilder;
import com.laytonsmith.PureUtilities.ArgumentParser.ArgumentBuilder.BuilderTypeNonFlag;
import com.laytonsmith.PureUtilities.Common.StringUtils;
import com.laytonsmith.core.AbstractCommandLineTool;
import com.laytonsmith.core.functions.Cmdline.prompt_char;
import com.laytonsmith.core.tool;

/**
 * Cmdline tool to create a DynamoDB table with the correct settings.
 */
@tool("x-msdynamodb-createtable")
public class TableCreator extends AbstractCommandLineTool {

	private static final String REGION = "region";
	private static final String TABLE_NAME = "table-name";
	private static final String ACCESS_KEY_ID = "access-key-id";
	private static final String ACCESS_KEY_SECRET = "access-key-secret";
	private static final String READ_CAPACITY_UNITS = "read-capacity-units";
	private static final String WRITE_CAPACITY_UNITS = "write-capacity-units";
	private static final String BILLING_MODE = "billing-mode";

	@Override
	public ArgumentParser getArgumentParser() {
		return ArgumentParser.GetParser()
				.addDescription("The tables created in DynamoDB must follow a specific format. While it is possible"
						+ " to create these tables yourself, this tool will automate the process for you.")
				.addArgument(new ArgumentBuilder()
						.setDescription("The region in which you wish to create this. As a special argument, if this"
								+ " argument is of the form \"<host>:<port>\", then the table will be created not in"
								+ " AWS, but in"
								+ " the provided url. Otherwise, an AWS region should be provided, one of: "
								+ StringUtils.Join(Regions.values(), ", ", ", or "))
						.setUsageName("region")
						.setRequired()
						.setName(REGION)
						.setArgType(BuilderTypeNonFlag.STRING))
				.addArgument(new ArgumentBuilder()
						.setDescription("The name of the table to be created. The table name must follow the naming"
								+ " rules found at"
								+ " https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html")
						.setUsageName("table name")
						.setRequired()
						.setName(TABLE_NAME)
						.setArgType(BuilderTypeNonFlag.STRING))
				.addArgument(new ArgumentBuilder()
						.setDescription("The access key id. This is optional, and if left off, it will use the"
								+ " credentials saved with the system. To set these up, see "
								+ "https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html.")
						.setUsageName("access key id")
						.setOptional()
						.setName(ACCESS_KEY_ID)
						.setArgType(BuilderTypeNonFlag.STRING)
						.setDefaultVal(null))
				.addArgument(new ArgumentBuilder()
						.setDescription("The access key secret. This is required if " + ACCESS_KEY_ID + " was provided,"
								+ " but not required otherwise.")
						.setUsageName("access key secret")
						.setOptional()
						.setName(ACCESS_KEY_SECRET)
						.setArgType(BuilderTypeNonFlag.STRING)
						.setDefaultVal(null))
				.addArgument(new ArgumentBuilder()
						.setDescription("The read capacity per second. This setting must be carefully considered, as"
								+ " depending on its setting, it can cause your bill to increase or decrease. See "
								+ "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html")
						.setUsageName("read capacity units")
						.setRequired()
						.setName(READ_CAPACITY_UNITS)
						.setArgType(BuilderTypeNonFlag.NUMBER))
				.addArgument(new ArgumentBuilder()
						.setDescription("The write capacity per second. This setting must be carefully considered, as"
								+ " depending on its setting, it can cause your bill to increase or decrease. See "
								+ "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html")
						.setUsageName("write capacity units")
						.setRequired()
						.setName(WRITE_CAPACITY_UNITS)
						.setArgType(BuilderTypeNonFlag.NUMBER))
				.addArgument(new ArgumentBuilder()
						.setDescription("The billing mode. See "
								+ "https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BillingModeSummary.html"
								+ " for more information. The billing mode must be one of: "
								+ StringUtils.Join(BillingMode.values(), ", ", ", or "))
						.setUsageName("billing mode")
						.setRequired()
						.setName(BILLING_MODE)
						.setArgType(BuilderTypeNonFlag.STRING));
	}

	@Override
	public void execute(ArgumentParser.ArgumentParserResults parsedArgs) throws Exception {
		String sRegion = parsedArgs.getStringArgument(REGION);
		String sTableName = parsedArgs.getStringArgument(TABLE_NAME);
		String sAccessKeyId = parsedArgs.getStringArgument(ACCESS_KEY_ID);
		String sAccessKeySecret = parsedArgs.getStringArgument(ACCESS_KEY_SECRET);
		if(sAccessKeyId == null && sAccessKeySecret != null || sAccessKeyId != null && sAccessKeySecret == null) {
			System.err.println(ACCESS_KEY_ID + " and " + ACCESS_KEY_SECRET + " must either both be provided, or"
					+ " neither");
			System.exit(1);
		}
		String host;
		int port = -1;
		Regions r;
		if(sRegion.contains(":")) {
			host = sRegion.split(":")[0];
			port = Integer.parseInt(sRegion.split(":")[1]);
			r = Regions.US_EAST_1;
		} else {
			host = null;
			r = Regions.fromName(sRegion);
		}
		BillingMode billingMode = BillingMode.fromValue(parsedArgs.getStringArgument(BILLING_MODE));
		long readCapacityUnits = parsedArgs.getNumberArgument(READ_CAPACITY_UNITS).longValue();
		long writeCapacityUnits = parsedArgs.getNumberArgument(WRITE_CAPACITY_UNITS).longValue();
		System.out.println("Using the following settings:");
		System.out.println("Host: " + host);
		System.out.println("Port: " + port);
		System.out.println("Region: " + r);
		if(sAccessKeyId == null) {
			System.out.println("Using built in credentials");
		} else {
			 System.out.println("Using provided credentials");
		}
		System.out.println("Table Name: " + sTableName);
		System.out.println("Read Capacity Units: " + readCapacityUnits);
		System.out.println("Write Capacity Units: " + writeCapacityUnits);
		System.out.println("Billing Mode: " + billingMode);
		char c = prompt_char.promptChar("If this looks correct, type Y to continue: ");
		if(c == 'y' || c == 'Y') {
			System.out.println("Creating table...");
			AmazonDynamoDB client = DynamoDBDataSource.buildClient("http", host, port, r, sAccessKeyId, sAccessKeySecret);
			DynamoDBDataSource.createTable(sTableName, client, readCapacityUnits, writeCapacityUnits, billingMode);
			System.out.println("Done.");
			System.exit(0);
		} else {
			System.out.println("Aborting operation.");
			System.exit(1);
		}
	}

}
