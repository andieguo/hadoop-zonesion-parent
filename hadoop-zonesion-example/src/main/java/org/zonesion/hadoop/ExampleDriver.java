/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zonesion.hadoop;

import org.apache.hadoop.util.ProgramDriver;
import org.zonesion.hadoop.api.HDFSAPI;
import org.zonesion.hbase.WordCountHbaseReader;
import org.zonesion.hbase.WordCountHbaseWriter;
import org.zonesion.hbase.api.HBaseAPI;

/**
 * A description of an example program based on its class and a human-readable
 * description.
 */
public class ExampleDriver {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("wordcount", WordCount.class,
					"A map/reduce program that counts the words in the input files.");
			pgd.addClass("pi", PiEstimator.class,
					"A map/reduce program that estimates Pi using monte-carlo method.");
			pgd.addClass("InvertedIndex", InvertedIndex.class,
					"A map/reduce program that inverted index method.");
			pgd.addClass("ProductJoinPrice", ProductJoinPrice.class,
					"A map/reduce program that ProductJoinPrice method.");
			pgd.addClass("SingletonTableJoin", SingletonTableJoin.class,
					"A map/reduce program that SingletonTableJoin method.");
			pgd.addClass("MapSideJoin", MapSideJoin.class,
					"A map/reduce program that MapSideJoin method.");
			pgd.addClass("EarthQuakesPerDate", EarthQuakesPerDate.class,
					"A map/reduce program that EarthQuakesPerDate method.");
			pgd.addClass("CompanyJoinAddress", CompanyJoinAddress.class,
					"A map/reduce program that CompanyJoinAddress method.");
			pgd.addClass("hdfs", HDFSAPI.class,
					"A map/reduce program that HDFSAPI method.");
			pgd.addClass("hbase", HBaseAPI.class,
					"A map/reduce program that HBaseAPI method.");
			pgd.addClass("WordCountHbaseReader", WordCountHbaseReader.class,
					"A map/reduce program that WordCountHbaseReader method.");
			pgd.addClass("WordCountHbaseWriter", WordCountHbaseWriter.class,
					"A map/reduce program that WordCountHbaseWriter method.");
			pgd.driver(argv);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
