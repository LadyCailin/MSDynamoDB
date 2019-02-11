/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.methodscript.msdynamodb;

import com.laytonsmith.PureUtilities.SimpleVersion;
import com.laytonsmith.PureUtilities.Version;
import com.laytonsmith.core.extensions.AbstractExtension;
import com.laytonsmith.core.extensions.MSExtension;

/**
 *
 */
@MSExtension("MSDynamoDB")
public class MSDynamoDB extends AbstractExtension {

	@Override
	public Version getVersion() {
		return new SimpleVersion(1, 0, 0, "SNAPSHOT");
	}

}
